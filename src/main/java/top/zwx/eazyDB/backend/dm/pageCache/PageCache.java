package top.zwx.eazyDB.backend.dm.pageCache;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import top.zwx.eazyDB.backend.dm.page.Page;
import top.zwx.eazyDB.backend.utils.Panic;
import top.zwx.eazyDB.common.Error;

public interface PageCache{

    public static final int PAGE_SIZE = 1 << 13;        //声明每一页的大小为8kb

    int newPage(byte[] initData);
    Page getPage(int pgno) throws Exception;
    void close();
    void release(Page page);

    void truncateByBgno(int maxPgno);
    int getPageNumber();
    void flushPage(Page pg);

    public static PageCacheImpl create(String path, long memory){
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()){
                Panic.panic(Error.FileExistsException);
            }
        }catch (Exception e){
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try{
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
