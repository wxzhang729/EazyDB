package top.zwx.eazyDB.backend.dm;

import top.zwx.eazyDB.backend.dm.logger.Logger;
import top.zwx.eazyDB.backend.dm.pageCache.PageCache;
import top.zwx.eazyDB.backend.tm.TransactionManager;

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

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    // updateLog:
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;                           //代表日志类型字段的起始位置
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;


    // insertLog:
    // [LogType] [XID] [Pgno] [Offset] [Raw]
    //LogType: 记录日志类型，表示这是一条插入日志（LOG_TYPE_INSERT）。
    //XID: 事务 ID，表示是哪一个事务执行了插入操作。
    //Pgno: 页号，表示插入操作所在的页面。
    //Offset: 偏移量，表示数据在页面中的位置。
    //Raw: 原始数据，表示插入的数据内容
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    private static void redoTransaction(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo li =
            }
        }
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {

    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }
}
