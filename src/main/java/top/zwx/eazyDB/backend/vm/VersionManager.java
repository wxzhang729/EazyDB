package top.zwx.eazyDB.backend.vm;

public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;

}
