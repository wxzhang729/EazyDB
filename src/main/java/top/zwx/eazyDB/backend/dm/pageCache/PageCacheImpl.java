package top.zwx.eazyDB.backend.dm.pageCache;


import top.zwx.eazyDB.backend.common.AbstractCache;
import top.zwx.eazyDB.backend.dm.page.Page;
import top.zwx.eazyDB.backend.dm.page.PageImpl;
import top.zwx.eazyDB.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.zwx.eazyDB.common.Error;

public class PageCacheImpl extends AbstractCache<Page> implements PageCache{

    private static final int MEM_MIN_LIM = 10;              //最小内存
    public static final String DB_SUFFIX = ".db";           //文件后缀

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;                      //用于原子性操作数据页号

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM){
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try{
            length = file.length();
        }catch (IOException e){
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
        this.fileLock = new ReentrantLock();
    }



    /**
     * 根据pageNumber从数据库文件中读取数据页，并包装成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int) key;
        long offset = PageCacheImpl.pageOffset(pgno);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try{
            fc.position(offset);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }
        fileLock.unlock();
        return new PageImpl(pgno,buf.array(),this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()){
            flush(pg);
            pg.setDirty(false);
        }
    }

    public int newPage(byte[] initData){
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno,initData,this);
        flush(pg);      //新建的页面需要写回到磁盘
        return pgno;
    }
    //这个方法用于将页面写回到磁盘
    private void flush(Page pg){
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try{
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());         //把page对象封装到buf中
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        }catch (IOException e){
            Panic.panic(e);
        }finally {
            fileLock.unlock();
        }
    }

    private static long pageOffset(int pgno) {
        return (pgno - 1) * PAGE_SIZE;
    }
}
