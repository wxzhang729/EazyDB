package top.zwx.eazyDB.backend.dm;

import top.zwx.eazyDB.backend.dm.dataItem.DataItem;
import top.zwx.eazyDB.backend.dm.logger.Logger;
import top.zwx.eazyDB.backend.dm.pageCache.PageCache;
import top.zwx.eazyDB.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;
    }
}
