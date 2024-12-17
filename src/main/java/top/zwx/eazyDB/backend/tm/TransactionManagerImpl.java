package top.zwx.eazyDB.backend.tm;

import top.zwx.eazyDB.backend.utils.Panic;
import top.zwx.eazyDB.backend.utils.Parser;
import top.zwx.eazyDB.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class TransactionManagerImpl {
    //XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    //每个事务的占用长度
    static final int XID_FIELD_SIZE = 1;
    //事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE = 0;        //正在进行状态
    private static final byte FIELD_TRAN_COMMITTED = 1;     //提交状态
    private static final byte FIELD_TRAN_ABORTED = 2;       //取消状态
    //超级事务
    public static final long SUPER_XID = 0;
    //XID文件后缀
    static final String XID_SUFFIX = ".xid";
    //随机访问读取和写入文件
    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;
    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }
    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它去计算文件的理论长度，对比实际长度
     */
    public void checkXIDCounter(){
        long fileLen = 0;
        try {
            fileLen = file.length();
        }catch (IOException e1){
            Panic.panic(Error.BadXIDFileException);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH){
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try{
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen){
            Panic.panic(Error.BadXIDFileException);
        }
    }
    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }
}
