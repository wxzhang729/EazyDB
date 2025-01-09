package top.zwx.eazyDB.backend.dm.dataItem;

public interface DataItem {
    public static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
