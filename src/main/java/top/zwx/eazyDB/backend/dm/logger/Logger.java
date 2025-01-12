package top.zwx.eazyDB.backend.dm.logger;

import top.zwx.eazyDB.backend.utils.Panic;
import top.zwx.eazyDB.backend.utils.Parser;
import top.zwx.eazyDB.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public interface Logger {
    void rewind();
    void truncate(long x) throws Exception;
    void log(byte[] data);
    byte[] next();              //读取下一条日志的有效部分
    void close();

    public static Logger create(String path){
        File f = new File(path + LoggerImpl.LOG_SUFFIX);
        try{
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
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        }catch (FileNotFoundException e){
            Panic.panic(e);
        }
        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try{
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new LoggerImpl(raf, fc, 0);
    }
}
