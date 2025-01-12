package top.zwx.eazyDB.backend.dm.pageIndex;

import top.zwx.eazyDB.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageIndex {
    //将一页划分为40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    public PageIndex(){
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for(int i = 0; i < INTERVALS_NO+1; i++){
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pgno, int freeSpace){
        lock.lock();
        try{
            int number = freeSpace/THRESHOLD;
            lists[number].add(new PageInfo(pgno, freeSpace));
        }finally {
            lock.unlock();
        }
    }

    //从页面索引中选择一个可以提供指定大小空闲空间的页面
    public PageInfo select(int spaceSize){
        lock.lock();
        try{
            int number = spaceSize/THRESHOLD;
            if(number <= INTERVALS_NO) number++;
            while(number <= INTERVALS_NO){
                if(lists[number].size() == 0){
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        }finally {
            lock.unlock();
        }
    }
}
