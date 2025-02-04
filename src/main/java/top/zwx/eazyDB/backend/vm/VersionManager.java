package top.zwx.eazyDB.backend.vm;

/**
 * VM层通过VersionManager，向上层提供api接口以及各种功能，对于VM上层的模块（是使用了VM层接口的上层模块），那么操作的都是Entry结构
 */
public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);
}
