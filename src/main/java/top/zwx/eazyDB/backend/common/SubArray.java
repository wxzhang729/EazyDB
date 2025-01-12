package top.zwx.eazyDB.backend.common;

//SubArray 是一个简单的辅助类，表示一个字节数组的子范围。
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
