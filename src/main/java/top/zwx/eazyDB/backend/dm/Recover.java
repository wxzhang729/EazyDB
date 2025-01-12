package top.zwx.eazyDB.backend.dm;

import com.google.common.primitives.Bytes;
import top.zwx.eazyDB.backend.common.SubArray;
import top.zwx.eazyDB.backend.dm.dataItem.DataItem;
import top.zwx.eazyDB.backend.dm.logger.Logger;
import top.zwx.eazyDB.backend.dm.page.Page;
import top.zwx.eazyDB.backend.dm.page.PageX;
import top.zwx.eazyDB.backend.dm.pageCache.PageCache;
import top.zwx.eazyDB.backend.tm.TransactionManager;
import top.zwx.eazyDB.backend.utils.Panic;
import top.zwx.eazyDB.backend.utils.Parser;

import java.util.*;
import java.util.Map;
import java.util.Map.Entry;
/**
 * 崩溃后事务恢复策略
 * 规定1：正在进行的事务，不会读取其他任何未提交的事务产生的数据。
 * 规定2：正在进行的事务，不会修改其他任何未提交的事务修改或产生的数据。
 * <p>
 * 并发情况下日志的恢复：
 * 1. 重做所有崩溃时已完成（committed 或 aborted）的事务
 * 2. 撤销所有崩溃时未完成（active）的事务
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }
    static class UpdateLogInfo{
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    // updateLog:
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;                           //代表 日志类型 字段的起始位置
    private static final int OF_XID = OF_TYPE + 1;                  //代表事务 ID（XID）字段的起始位置。占8字节
    private static final int OF_UPDATE_UID = OF_XID + 8;           //代表更新操作的唯一标识符（UID）字段的起始位置。占8字节
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;    //代表旧值（OldRaw）和新值（NewRaw）字段的起始位置。

    public static byte[] insertLog(long xid, Page pg, byte[] raw){
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, xidRaw, offsetRaw, offsetRaw, raw);
    }

    //撤销（UNDO）事务，将所有处于 未完成状态（active） 的事务所做的操作回滚
    private static void undoTransactions(TransactionManager tm, Logger lg, PageCache pc){
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while(true){
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)){
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(tm.isActive(xid)){
                    if(!logCache.containsKey(xid)){
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }else{
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)){
                    if(!logCache.containsKey(xid)){
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }
        //对所有active log进行倒序undo
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for(int i = logs.size() - 1; i >= 0; i--){
                byte[] log = logs.get(i);
                if(isInsertLog(log)){
                    doInsertLog(pc, log, UNDO);
                }else{
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }


    // insertLog:
    // [LogType] [XID] [Pgno] [Offset] [Raw]
    //LogType: 记录日志类型，表示这是一条插入日志（LOG_TYPE_INSERT）。
    //XID: 事务 ID，表示是哪一个事务执行了插入操作。
    //Pgno: 页号，表示插入操作所在的页面。
    //Offset: 偏移量，表示数据在页面中的位置。
    //Raw: 原始数据，表示插入的数据内容
    private static final int OF_INSERT_PGNO = OF_XID + 8;           //页号，占4字节
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4; //偏移量，占2字节
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;  //原始数据
    //重做（REDO）事务，将所有已完成（commit） 的事务所做的操作回滚
    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)){
                    doInsertLog(pc,log,REDO);
                }
            }else{
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)){
                    doUpdateLog(pc,log,REDO);
                }
            }
        }
    }

    //更新日志的恢复操作
    private static void doUpdateLog(PageCache pc, byte[] log, int flag){
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO){
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        }else{
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try{
            pg = pc.getPage(pgno);
        }catch (Exception e){
            Panic.panic(e);
        }
        try{
            PageX.recoverUpdate(pg,raw,offset);
        }finally {
            pg.release();
        }
    }

    public static byte[] updateLog(long xid, DataItem di){
        byte[] logType = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    //从 更新日志的字节数组 中解析出插入操作的相关信息
    private static UpdateLogInfo parseUpdateLog(byte[] log){
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length*2);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try{
            pg = pc.getPage(li.pgno);
        }catch (Exception e){
            Panic.panic(e);
        }
        try{
            if(flag == UNDO){
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg,li.raw,li.offset);
        }finally {
            pg.release();
        }
    }

    //从 插入日志的字节数组 中解析出插入操作的相关信息
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        return li;
    }

    //判断给定日志是否是一条插入日志
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }
}
