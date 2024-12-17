package top.zwx.eazyDB.backend.tm;

public interface TransactionManager {
    long begin();                   //开启一个新事务
    void commit(long xid);          //提交一个事务
    void abort(long xid);           //取消一个事务
    boolean isActive(long xid);     //查询一个事务的状态是否是正在进行的状态
    boolean isCommitted(long xid);  //查询一个事物的状态是否是已提交
    boolean isAborted(long xid);    //查询一个事务的状态是否是已取消
    void close();                   //关闭事务
}
