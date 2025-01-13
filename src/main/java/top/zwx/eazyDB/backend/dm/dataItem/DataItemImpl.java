package top.zwx.eazyDB.backend.dm.dataItem;

import top.zwx.eazyDB.backend.common.SubArray;
import top.zwx.eazyDB.backend.dm.DataManager;
import top.zwx.eazyDB.backend.dm.DataManagerImpl;
import top.zwx.eazyDB.backend.dm.Recover;
import top.zwx.eazyDB.backend.dm.logger.Logger;
import top.zwx.eazyDB.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 向上层提供的数据抽象
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {
    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rlock;
    private Lock wlock;
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.pg = pg;
        this.uid = uid;
        this.dm = dm;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        wlock = lock.writeLock();
        rlock = lock.readLock();
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end);
    }

    //用于事务回滚
    @Override
    public void before() {
        wlock.lock();
        pg.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wlock.unlock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid,this);
        wlock.lock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wlock.lock();
    }

    @Override
    public void unLock() {
        wlock.unlock();
    }

    @Override
    public void rLock() {
        rlock.lock();
    }

    @Override
    public void rUnLock() {
        rlock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }





    public boolean isValid(){
        return raw.raw[raw.start+OF_VALID] == (byte) 0;
    }
}
