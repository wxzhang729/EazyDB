package top.zwx.eazyDB.backend.vm;

import top.zwx.eazyDB.backend.common.AbstractCache;
import top.zwx.eazyDB.backend.dm.DataManager;
import top.zwx.eazyDB.backend.dm.page.Page;
import top.zwx.eazyDB.backend.tm.TransactionManager;
import top.zwx.eazyDB.backend.tm.TransactionManagerImpl;
import top.zwx.eazyDB.backend.utils.Panic;
import top.zwx.eazyDB.common.Error;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager {
    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;
    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    //读取一个entry，注意判断可见性
    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock(); // 获取锁，防止并发问题
        Transaction t = activeTransaction.get(xid); // 从活动事务中获取事务对象
        lock.unlock(); // 释放锁

        if (t.err != null) { // 如果事务已经出错，那么抛出错误
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid); // 尝试获取数据项
        } catch (Exception e) {
            if (e == Error.NullEntryException) { // 如果数据项不存在，那么返回null
                return null;
            } else { // 如果出现其他错误，那么抛出错误
                throw e;
            }
        }
        try {
            // 在事务隔离级别中讲解了该方法
            if (Visibility.isVisible(tm, t, entry)) { // 如果数据项对当前事务可见，那么返回数据项的数据
                return entry.data();
            } else { // 如果数据项对当前事务不可见，那么返回null
                return null;
            }
        } finally {
            entry.release(); // 释放数据项
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);     //从活动事务中获取事务
        lock.unlock();

        if (t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);     //从活跃事务中获取事务对象
        lock.unlock();
        if (t.err != null) {
            throw t.err;
        }
        Entry entry = null;
        try{
            //尝试获取数据项
            entry = super.get(uid);
        }catch(Exception e){
            //如果数据项不存在，就返回false
            if (e == Error.NullEntryException) {
                return false;
            }else{
                throw e;
            }
        }
        try{
            //如果数据项对当前事务不可见，返回false
            if (!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try{
                //尝试为数据项添加锁
                l = lt.add(xid, uid);
            }catch(Exception e){
                //如果出现了并发更新的错误，那么中止事务，并抛出错误
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            //如果成功获取到锁，那么锁定并立即解锁
            if(l != null){
                l.lock();
                l.unlock();
            }
            //如果数据项已经被当前事务删除，那么返回false
            if(entry.getXmax() == xid){
                return false;
            }
            //如果数据项的版本被跳过，那么中止事务，并抛出错误
            if(Visibility.isVersionSkip(tm, t, entry)){
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            //设置数据项的xmax为当前事务的ID，表示数据项被当前事务删除
            entry.setXmax(xid);
            return true;
        }finally{
            //释放数据项
            entry.release();
        }
    }


    public void releaseEntry(Entry entry){
        super.release(entry.getUid());
    }

    //创建事务，将事务添加到活动事务的映射中，并返回新事务的ID
    @Override
    public long begin(int level) {
        lock.lock(); // 获取锁，防止并发问题
        try {
            long xid = tm.begin(); // 调用事务管理器的begin方法，开始一个新的事务，并获取事务ID
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction); // 创建一个新的事务对象
            activeTransaction.put(xid, t); // 将新的事务对象添加到活动事务的映射中
            return xid; // 返回新的事务ID
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);     //获取事务
        lock.unlock();
        //如果有错误，则抛出
        try{
            if(t.err != null){
                throw t.err;
            }
        }catch(NullPointerException n){
            System.out.println(xid);
            System.out.println(activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock(); // 获取锁，防止并发问题
        activeTransaction.remove(xid); // 1、从活动事务中移除这个事务
        lock.unlock(); // 释放锁

        lt.remove(xid); // 2、从锁表中移除这个事务的锁
        tm.commit(xid); // 3、调用事务管理器的commit方法，进行事务的提交操作
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if(entry == null){
            throw Error.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }

    @Override
    public void abort(long xid) {
        // 调用内部的abort方法，autoAborted参数为false表示这不是一个自动中止的事务
        internAbort(xid, false);
    }

    // 内部的abort方法，处理事务的中止
    private void internAbort(long xid, boolean autoAborted) {
        // 获取锁，防止并发问题
        lock.lock();
        // 从活动事务中获取事务对象
        Transaction t = activeTransaction.get(xid);
        // 如果这不是一个自动中止的事务，那么从活动事务中移除这个事务
        if (!autoAborted) {
            activeTransaction.remove(xid);
        }
        // 释放锁
        lock.unlock();

        // 如果事务已经被自动中止，那么直接返回，不做任何处理
        if (t.autoAborted) return;
        // 从锁表中移除这个事务的锁
        lt.remove(xid);
        // 调用事务管理器的abort方法，进行事务的中止操作
        tm.abort(xid);
    }


}
