package top.zwx.eazyDB.backend.dm.pageCache;

import top.zwx.eazyDB.backend.dm.page.Page;

public interface PageCache{

    public static final int PAGE_SIZE = 1 << 13;        //声明每一页的大小为8kb

    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);

    void truncateByBgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);
}
