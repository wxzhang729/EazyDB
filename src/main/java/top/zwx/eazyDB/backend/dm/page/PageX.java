package top.zwx.eazyDB.backend.dm.page;

import top.zwx.eazyDB.backend.dm.pageCache.PageCache;
import top.zwx.eazyDB.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 */
public class PageX {
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;  //可用空间为8-2

    public static byte[] initRaw(){
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw,OF_DATA);
        return raw;
    }
    //用于设置页面的空闲位置偏移量（FSO）
    private static void setFSO(byte[] raw, short ofData){
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    public static short getFSO(Page pg){
        return getFSO(pg.getData());
    }
    //用于获取给定字节数组raw中表示页面空闲位置偏移量（FSO）的short值
    private static short getFSO(byte[] raw){
        return Parser.parseShort(Arrays.copyOfRange(raw,0,2));
    }

    //将raw插入pg中，返回插入位置
    public static short insert(Page pg, byte[] raw){
        pg.setDirty(true);
        short offset = getFSO(pg.getData());
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
        setFSO(pg.getData(),(short)(offset+raw.length));
        return offset;
    }

    //获取页面的空闲空间大小
    public static int getFreeSpace(Page pg){
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /**
     * recoverInsert() 和 recoverUpdate() 用于在数据库崩溃后重新打开时，恢复例程直接插入数据以及修改数据使用
     */
    //将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg, byte[] raw, short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);

        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length){
            setFSO(pg.getData(),(short)(offset+raw.length));
        }
    }

    //将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset){
        pg.setDirty(true);
        System.arraycopy(raw,0,pg.getData(),offset,raw.length);
    }
}
