package top.zwx.eazyDB.backend.vm;

import com.google.common.primitives.Bytes;
import top.zwx.eazyDB.backend.common.SubArray;
import top.zwx.eazyDB.backend.dm.dataItem.DataItem;
import top.zwx.eazyDB.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出entry
 * entry结构
 * [XMIN] [XMAX] [data]
 * XMIN 是创建该条记录（版本）的事务编号, XMAX 则是删除该条记录（版本）的事务编号。DATA 就是这条记录持有的数据
 */
public class Entry {
    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager vm;

    public static Entry newEntry(VersionManager vm, DataItem dataItem,long uid) {
        if(dataItem == null){
            return null;
        }
        Entry entry = new Entry();
        entry.dataItem = dataItem;
        entry.uid = uid;
        entry.vm = vm;
        return entry;
    }

    public static Entry loadEntry(VersionManager vm, long uid, DataItem dataItem) {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di,uid);
    }

    public void remove(){
        dataItem.release();
    }

    //创建entry记录
    public static byte[] wrapEntryRaw(long xid, byte[] data){
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin,xmax,data);
    }

    //以拷贝的形式返回内容
    public byte[] data(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
            return data;
        }finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid){
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid),0,sa.raw, sa.start + OF_XMAX, 8);
        }finally {
            dataItem.after(xid);
        }
    }

    public long getXmax(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start+OF_DATA));
        }finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin(){
        dataItem.rLock();
        try{
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start+OF_XMAX));
        }finally {
            dataItem.rUnLock();
        }
    }
}
