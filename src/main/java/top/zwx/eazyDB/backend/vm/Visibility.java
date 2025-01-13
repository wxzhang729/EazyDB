package top.zwx.eazyDB.backend.vm;

import top.zwx.eazyDB.backend.tm.TransactionManager;

public class Visibility {

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e){
        if(t.level == 0){
            return readCommitted(tm, t, e);
        }else{
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 读已提交逻辑：
     * ①数据项是由当前事务创建，且还没有被删除，那么这个版本的数据是可用的，
     * ②数据的创建事务已经提交，且还没被删除；或者数据项被其他事务标记为删除，但是该事务还未提交
     * ps：对第二点进行补充，就是先判断当前数据的创建事务xmin是否已经提交，如果没提交，该版本直接不可读，如果提交了，继续往下走，
     *      然后判断删除事务xmax是否是0，是0表示没有事务删除该数据，数据对当前事务可见，返回true
     *  或  如果删除事务存在且不等于0，那就判断这个删除事务是不是自己，如果是，则数据对当前事务不可见，
     *      如果不是，则判断删除事务是否已经提交，提交了说明该数据已经被跟新或者删除了，因此这个版本不可见，反之数据对当前事务可见
     * (XMIN == Ti and                             // 由Ti创建且
     *     XMAX == NULL                            // 还未被删除
     * )
     * or                                          // 或
     * (XMIN is commited and                       // 由一个已提交的事务创建且
     *     (XMAX == NULL or                        // 尚未删除或
     *     (XMAX != Ti and XMAX is not commited)   // 由一个未提交的事务删除
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0){
            return true;
        }

        if(tm.isCommitted(xmin)){
            if(xmax == 0) return true;
            if(xmax != xid){
                if(!tm.isCommitted(xmax)){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * (XMIN == Ti and                 // 由Ti创建且
     *  (XMAX == NULL                  // 尚未被删除
     * ))
     * or                              // 或
     * (XMIN is commited and           // 由一个已提交的事务创建且
     *  XMIN < XID and                 // 这个事务小于Ti且
     *  XMIN is not in SP(Ti) and      // 这个事务在Ti开始前提交且
     *  (XMAX == NULL or               // 尚未被删除或
     *   (XMAX != Ti and               // 由其他事务删除但是
     *    (XMAX is not commited or     // 这个事务尚未提交或
     * XMAX > Ti or                    // 这个事务在Ti开始之后才开始或
     * XMAX is in SP(Ti)               // 这个事务在Ti开始前还未提交
     * ))))
     * @param tm
     * @param t
     * @param e
     * @return
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e){
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        if(xmin == xid && xmax == 0) return true;       //数据是当前事务创建的，并且尚未被删除。可见

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)){
            if(xmax == 0) return true;
            if(xmax != xid){
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)){
                    return true;
                }
            }
        }
        return false;
    }
}
