package top.zwx.eazyDB.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import top.zwx.eazyDB.backend.common.SubArray;
import top.zwx.eazyDB.backend.dm.DataManagerImpl;
import top.zwx.eazyDB.backend.dm.page.Page;
import top.zwx.eazyDB.backend.utils.Parser;
import top.zwx.eazyDB.backend.utils.Types;

import java.util.Arrays;

//向上层提供的数据抽象
public interface DataItem {
    SubArray data();

    void before();
    void after(long xid);
    void unBefore();
    void release();

    void lock();
    void unLock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);//将许多数组的值连接到一个数组
    }

    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }

    //从页面的offset处解析出dataitem
    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw,offset+DataItemImpl.OF_SIZE, offset+DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length] ,pg, uid, dm);
    }
}
