package top.zwx.eazyDB.backend.tm;

import top.zwx.eazyDB.backend.utils.Panic;
import top.zwx.eazyDB.common.Error;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface TransactionManager {
    long begin();                   //开启一个新事务
    void commit(long xid);          //提交一个事务
    void abort(long xid);           //取消一个事务
    boolean isActive(long xid);     //查询一个事务的状态是否是正在进行的状态
    boolean isCommitted(long xid);  //查询一个事物的状态是否是已提交
    boolean isAborted(long xid);    //查询一个事务的状态是否是已取消
    void close();                   //关闭事务

    public static TransactionManagerImpl create(String path){
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!f.createNewFile()){  //创建新文件夹
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
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        //写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try{
            fc.position(0);
            fc.write(buf);
        }catch (Exception e){
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf,fc);
    }

    public static TransactionManagerImpl open(String path){
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()){
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()){
            Panic.panic(Error.FileCannotRWException);
        }
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f,"rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new TransactionManagerImpl(raf,fc);
    }
}
