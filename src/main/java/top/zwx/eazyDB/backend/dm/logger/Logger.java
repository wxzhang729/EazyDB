package top.zwx.eazyDB.backend.dm.logger;

public interface Logger {
    void rewind();
    void truncate(long x) throws Exception;
}
