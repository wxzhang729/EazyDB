package top.zwx.eazyDB.backend.vm;

import top.zwx.eazyDB.common.Error;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 维护了一个依赖等待图
 */
public class LockTable {
    //键是事务id，值是所获得资源id的列表
    private Map<Long, List<Long>> x2u;          //某个XID已经获得的资源的uid列表
    //键是资源id，值是持有该资源的事务id
    private Map<Long, Long> u2x;                //UID被某个XID持有
    //键是资源id，值是等待资源的事务id列表
    private Map<Long, List<Long>> wait;         //正在等待UID的xid列表
    //键是资源id，值是该事务的锁对象
    private Map<Long, Lock> waitLock;           //正在等待资源的xid的锁
    //键是事务id，值是该事务等待的资源id
    private Map<Long, Long> waitU;              //XID正在等待的UID
    private Lock lock;

    // Add 添加一个事务ID和资源ID的映射关系，返回一个锁对象，如果发生死锁，返回错误
    // 不需要等待则返回null，否则返回锁对象或者会造成死锁则抛出异常
    public Lock add(long xid, long uid) throws Exception {
        lock.lock(); // 锁定全局锁
        try {
            // 检查x2u是否已经拥有这个资源
            if (isInList(x2u, xid, uid)) {
                return null; // 如果已经拥有，直接返回null
            }
            // 检查UID资源是否已经被其他XID事务持有
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid); // 如果没有被持有，将资源分配给当前事务
                putIntoList(x2u, xid, uid); // 将资源添加到事务的资源列表中
                return null; // 返回null
            }
            // 如果资源已经被其他事务持有，将当前事务添加到等待列表中(waitU是一个map，键是事务ID，值是资源ID，代表事务正在等待的资源)
            waitU.put(xid, uid);
            //反过来，将资源添加到等待列表中(wait是一个map，键是资源ID，值是事务ID列表，代表正在等待该资源的事务)
            putIntoList(wait, uid, xid);
            // 检查是否存在死锁
            if (hasDeadLock()) {
                waitU.remove(xid); // 如果存在死锁，从等待列表中移除当前事务
                removeFromList(wait, uid, xid);//从资源的等待列表中移除当前事务
                throw Error.DeadlockException; // 抛出死锁异常
            }
            // 如果不存在死锁，为当前事务创建一个新的锁，并锁定它
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l); // 将新的锁添加到等待锁列表中
            return l; // 返回新的锁
        } finally {
            lock.unlock(); // 解锁全局锁
        }
    }


    // isInList 判断xid是否在x2u中,如果已经持有，返回true，否则返回false,uid0是事务id
    //x2u:某个XID(事务)已经获得的资源的UID列表
    public boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1){
        List<Long> l = listMap.get(uid0);
        if(l == null){return false;}
        Iterator<Long> i = l.iterator();
        while(i.hasNext()){
            long e = i.next();
            if(e == uid1){
                return true;
            }
        }
        return false;
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1){
        if(!listMap.containsKey(uid0)){
            listMap.put(uid0, new ArrayList<Long>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    //检查是否存在死锁
    private boolean hasDeadLock(){
        xidStamp = new HashMap<Long, Integer>();
        stamp = 1;
        for(long xid : x2u.keySet()){
            Integer s = xidStamp.get(xid);
            if(s != null && s > 0){
                continue;
            }
            stamp++;
            if(dfs(xid)){
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid){
        Integer stp = xidStamp.get(xid);
        if(stp != null && stp == stamp){
            return true;
        }
        if(stp != null && stp < stamp){
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if(uid != null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    //传入wait，即等待资源的事务列表，键是资源id，值是等待资源的事务
    //发生死锁的时候调用，从等待资源的事务中删除掉该事务
    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1){
        List<Long> l = listMap.get(uid0);
        if(l == null){return;}
        Iterator<Long> i = l.iterator();
        while(i.hasNext()){
            long e = i.next();
            if(e == uid1){
                i.remove();
                break;
            }
        }
        if(l.size() == 0){          //没有等待该资源的事务
            listMap.remove(uid0);
        }
    }

    // Remove 当一个事务commit或者abort时，就会释放掉它自己持有的锁，并将自身从等待图中删除
    public void remove(long xid) {
        lock.lock(); // 获取全局锁
        try {
            List<Long> l = x2u.get(xid); // 从x2u映射中获取当前事务ID已经获得的资源的UID列表
            if (l != null) {
                while (l.size() > 0) {
                    Long uid = l.remove(0); // 获取并移除列表中的第一个资源ID
                    selectNewXID(uid); // 从等待队列中选择一个新的事务ID来占用这个资源
                }
            }
            waitU.remove(xid); // 从waitU映射中移除当前事务ID
            x2u.remove(xid); // 从x2u映射中移除当前事务ID
            waitLock.remove(xid); // 从waitLock映射中移除当前事务ID

        } finally {
            lock.unlock(); // 解锁全局锁
        }
    }

    // 从等待队列中选择一个xid来占用uid
    //移除当前资源id
    private void selectNewXID(long uid) {
        u2x.remove(uid); // 从u2x映射中移除当前资源ID
        List<Long> l = wait.get(uid); // 从wait映射中获取当前资源ID的等待队列
        if (l == null) return; // 如果等待队列为空，立即返回
        assert l.size() > 0; // 断言等待队列不为空

        // 遍历等待队列
        while (l.size() > 0) {
            long xid = l.remove(0); // 获取并移除队列中的第一个事务ID
            // 检查事务ID是否在waitLock映射中
            if (!waitLock.containsKey(xid)) {
                continue; // 如果不在，跳过这个事务ID，继续下一个
            } else {
                u2x.put(uid, xid); // 将事务ID和资源ID添加到u2x映射中
                Lock lo = waitLock.remove(xid); // 从waitLock映射中移除这个事务ID
                waitU.remove(xid); // 从waitU映射中移除这个事务ID
                lo.unlock(); // 解锁这个事务ID的锁
                break; // 跳出循环
            }
        }

        // 如果等待队列为空，从wait映射中移除当前资源ID
        if (l.size() == 0) wait.remove(uid);
    }

}
