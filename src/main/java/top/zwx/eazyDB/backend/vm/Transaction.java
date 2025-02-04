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
    // 创建一个新的事务：设置事务的隔离级别、隔离级别是可重复读时创建快照
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        // 设置事务ID
        t.xid = xid;
        // 设置事务隔离级别
        t.level = level;
        // 如果隔离级别不为0，创建快照
        if (level != 0) {
            t.snapshot = new HashMap<>();
            // 将活跃事务的ID添加到快照中
            for (Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        // 返回新创建的事务
        return t;
    }

    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID){
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
