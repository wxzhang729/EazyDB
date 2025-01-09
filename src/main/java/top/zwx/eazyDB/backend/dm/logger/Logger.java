package top.zwx.eazyDB.backend.dm.logger;

public interface Logger {
    void rewind();
    void truncate(long x) throws Exception;
    void log(byte[] data);
    byte[] next();              //读取下一条日志的有效部分
}
