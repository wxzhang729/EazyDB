package top.zwx.eazyDB.backend.dm.logger;

import top.zwx.eazyDB.backend.utils.Panic;
import top.zwx.eazyDB.backend.utils.Parser;
import top.zwx.eazyDB.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件读写
 *
 * 日志文件标准格式为：
 * [XChecksum][Log1][Log2]...[LogN][BadTail]
 * XChecksum是一个四字节的整数，是对所有后面日志计算的校验和
 * log1~logN是正常日志，BadTail是数据库崩溃后还没来得及写的日志
 *
 * 每条日志的格式为：
 * [Size][Checksum][Data]
 * Size是一个四字节的整数，用于标识Data的长度
 * Checksum是该条日志的校验和
 */
public class LoggerImpl implements Logger {
    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;                       //size的偏移量从0开始
    private static final int OF_CHECKSUM = OF_SIZE + 4;         //checksum的偏移量从size+4开始
    private static final int OF_DATA = OF_CHECKSUM + 4;         //data偏移量从Checksum+4开始

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;          //当前日志指针的位置
    private long fileSize;          //初始化时记录，log操作不更新
    private long xChecksum;         //校验和

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum){
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    private int calChecksum(int xCheck, byte[] log){
        for(byte b : log){
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 不断从文件中读取下一条日志，将其中的data解析出来
     *
     * 检查当前文件指针是否可以读取完整的日志条目。
     * 读取日志条目的 Size 字段，计算数据的长度。
     * 判断当前日志条目是否完整（即 Size + Checksum + Data 是否超出文件的大小）。
     * 读取整个日志条目。
     * 校验 Data 部分的校验和，确保数据的完整性。
     * 更新文件指针，指向下一个日志条目。
     * 返回有效的日志数据。
     * @return
     */
    private byte[] internNext(){
        //确保日志还有data的空间
        if(position + OF_DATA >= fileSize){
            return null;
        }
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try{
            fc.position(position);
            fc.read(tmp);
        }catch (IOException e){
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        //确保整条日志的大小是否超出文件大小
        if(position + size + OF_DATA > fileSize){
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try{
            fc.position(position);
            fc.read(buf);
        }catch (IOException e){
            Panic.panic(e);
        }

        byte[] log = buf.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log,OF_DATA, log.length));    //计算日志的校验和
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log,OF_CHECKSUM, OF_DATA));          //拿到普通日志中的checksum
        if(checkSum1 != checkSum2){
            return null;
        }
        position += log.length;
        return log;
    }

    //检查并移除bad tail
    private void checkAndRemoveTail(){
        rewind();

        int xCheck = 0;
        while(true){
            byte[] log = internNext();
            if(log == null){
                break;
            }
            xCheck = calChecksum(xCheck, log);
        }
        if(xCheck != xChecksum){
            Panic.panic(Error.BadLogFileException);
        }

        try{
            truncate(position);
        }catch (Exception e){
            Panic.panic(e);
        }
        try{
            file.seek(position);
        }catch (IOException e){
            Panic.panic(e);
        }
        rewind();
    }

    @Override
    public void truncate(long x) throws IOException {
        lock.lock();
        try{
            fc.truncate(x);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }
}
