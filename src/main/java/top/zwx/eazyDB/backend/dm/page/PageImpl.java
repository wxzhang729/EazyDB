package top.zwx.eazyDB.backend.dm.page;

import top.zwx.eazyDB.backend.dm.pageCache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PageImpl implements Page{
    private int pageNumber;                 //页面的页号
    private byte[] data;                    //这个页实际包含的字节数据
    /**
     * dirty 标志着这个页面是否是脏页面，在缓存驱逐的时候，脏页面需要被写回磁盘
     * 如果在驱逐脏页面时不将其写回磁盘，当缓存被覆盖或者系统发生故障时，这些修改将丢失，导致数据库的状态不一致或数据丢失
     */
    private boolean dirty;
    private Lock lock;

    private PageCache pc;                   //用来方便在拿到 Page 的引用时可以快速对这个页面的缓存进行释放操作。

    public PageImpl(int pageNumber, byte[] data, PageCache pc) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pc = pc;
        lock = new ReentrantLock();
    }
}
