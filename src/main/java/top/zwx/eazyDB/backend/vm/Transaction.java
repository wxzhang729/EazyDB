package top.zwx.eazyDB.backend.vm;

import top.zwx.eazyDB.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

//vm对事务的抽象
public class Transaction {
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    //创建一个事务，记录当前事务中活跃的事务id
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;        //隔离级别
        if (level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID){
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
