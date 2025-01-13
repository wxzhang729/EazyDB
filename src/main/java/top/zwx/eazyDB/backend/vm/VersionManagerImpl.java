package top.zwx.eazyDB.backend.vm;

import top.zwx.eazyDB.backend.common.AbstractCache;
import top.zwx.eazyDB.backend.dm.DataManager;
import top.zwx.eazyDB.backend.tm.TransactionManager;
import top.zwx.eazyDB.common.Error;

import java.util.Map;
import java.util.concurrent.locks.Lock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {
    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;


    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null){
            throw t.err;
        }

        Entry entry = null;
        try{
            entry = super.get(uid);
        }catch(Exception e){
            if(e == Error.NullEntryException){
                return null;
            }else{
                throw e;
            }
        }
        try{
            if(Visibility.isVisible(tm, t, entry)){
                return entry.data();
            }else{
                return null;
            }
        }finally {
            entry.release;
        }
    }
}
