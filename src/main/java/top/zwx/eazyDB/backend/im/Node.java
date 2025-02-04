package top.zwx.eazyDB.backend.im;

import top.zwx.eazyDB.backend.common.SubArray;
import top.zwx.eazyDB.backend.dm.dataItem.DataItem;
import top.zwx.eazyDB.backend.tm.TransactionManagerImpl;
import top.zwx.eazyDB.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构
 * [LeafFlag][KeyNumber][SiblingUid]
 * 1字节、2字节、8字节
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 * 都是8字节
 * LeafFlag：标记该节点是否是叶子节点
 * KeyNumber：该节点中key的个数
 * SiblingUid：兄弟节点存储在 DM 中的 UID
 * Son：指向子节点的指针
 * Key：索引键，通过索引键可以定位到具体的节点
 */
public class Node {
    static final int IS_LEAF_OFFSET = 0;
    static final int NO_KEYS_OFFSET = IS_LEAF_OFFSET+1;
    static final int SIBLING_OFFSET = NO_KEYS_OFFSET+2;
    static final int NODE_HEADER_SIZE = SIBLING_OFFSET+8;

    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2*8)*(BALANCE_NUMBER*2+2);

    BPlusTree tree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf){
            raw.raw[raw.start + IS_LEAF_OFFSET] = (byte)1;      //如果是叶子节点，将LeafFlag设置为1，否则为0
        }
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys),0, raw.raw, raw.start + NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start + NO_KEYS_OFFSET, raw.start + NO_KEYS_OFFSET+2));
    }

    //设置SiblingUid为兄弟节点的uid
    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling),0, raw.raw, raw.start + SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start + SIBLING_OFFSET+8));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start + NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NO_KEYS_OFFSET, from.end-offset);
    }

    static byte[] newRootRaw(long left, long right, long key){
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);

        setRawIsLeaf(raw, false); // 设置标志位，不是叶子节点
        setRawNoKeys(raw, 2); // 设置 key 的数量为2，有两个子节点
        setRawSibling(raw, 0); // 设置兄弟节点的 uid，根节点无邻节点
        setRawKthSon(raw, left, 0); // 设置第0个子节点的 uid 为 left，相当于指向叶子节点的指针
        setRawKthKey(raw, key, 0); // 设置第0个子节点的 key 为 key，这个就是索引键，通过索引键可以定位到具体的节点
        setRawKthSon(raw, right, 1); // 设置第1个子节点的 uid 为 right
        setRawKthKey(raw, Long.MAX_VALUE, 1); // 设置第1个子节点的 key 为 MAX_VALUE
        return raw.raw;
    }

    //叶子节点
    static byte[] newNilRootRaw(){
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw, true);
        setRawNoKeys(raw, 0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    class SearchNextRes{
        long uid;
        long siblingUid;
    }

    public SearchNextRes searchNext(long key) {
        // 1. 获取读锁，确保线程安全
        dataItem.rLock();
        try {
            // 2. 初始化结果对象
            SearchNextRes res = new SearchNextRes();
            // 3. 获取当前节点的键数量
            int noKeys = getRawNoKeys(raw);

            // 4. 遍历所有键（有序）
            for (int i = 0; i < noKeys; i++) {
                // 5. 获取第i个键的值
                long ik = getRawKthKey(raw, i);

                // 6. 如果目标key小于当前键ik：
                if (key < ik) {
                    // 6.1 返回对应的子节点UID（路由到该子节点继续搜索）
                    res.uid = getRawKthSon(raw, i);
                    // 6.2 兄弟节点UID设为0（表示不需要横向查找）
                    res.siblingUid = 0;
                    return res; // 立即返回结果
                }
            }

            // 7. 如果遍历完所有键仍未找到更大的key：
            // 7.1 子节点UID设为0（表示没有合适的子节点）
            res.uid = 0;
            // 7.2 获取兄弟节点UID（用于横向遍历叶子节点或父节点调整）
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            // 8. 无论成功与否，最终释放读锁
            dataItem.rUnLock();
        }
    }

    //在当前节点进行范围查找，范围是 [leftKey, rightKey]
    class LeafSearchRangeRes {
        List<Long> uids;
        long siblingUid;
    }

    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        // 1. 获取读锁，确保线程安全
        dataItem.rLock();
        try {
            // 2. 初始化结果对象和临时变量
            int noKeys = getRawNoKeys(raw);   // 获取当前节点的键总数
            int kth = 0;                      // 遍历指针，从第一个键开始

            // 3. 定位第一个 >= leftKey 的键的位置
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);  // 获取第kth个键
                if (ik >= leftKey) {               // 找到第一个 >= leftKey 的键
                    break;
                }
                kth++;                             // 继续向右查找
            }

            // 4. 收集满足 [leftKey, rightKey] 范围的键对应的UID
            List<Long> uids = new ArrayList<>();
            while (kth < noKeys) {
                long ik = getRawKthKey(raw, kth);  // 获取当前键
                if (ik <= rightKey) {              // 键仍在范围内
                    uids.add(getRawKthSon(raw, kth)); // 记录该键对应的数据UID
                    kth++;                         // 继续下一个键
                } else {                           // 超出右边界，终止收集
                    break;
                }
            }

            // 5. 处理兄弟节点（叶子节点横向链表）
            long siblingUid = 0;
            if (kth == noKeys) {                   // 所有键都 <= rightKey
                siblingUid = getRawSibling(raw);   // 获取兄弟节点UID（可能跨节点继续查找）
            }

            // 6. 封装结果
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            res.uids = uids;                       // 当前节点符合条件的UID列表
            res.siblingUid = siblingUid;           // 需要继续查找的兄弟节点UID
            return res;

        } finally {
            // 7. 无论如何都释放读锁
            dataItem.rUnLock();
        }
    }

    static Node loadNode(BPlusTree bTree, long uid) throws Exception{
        DataItem di = bTree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.tree = bTree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    static boolean getRawIsLeaf(SubArray raw) {
        return raw.raw[raw.start + IS_LEAF_OFFSET] == (byte)1;
    }

    public boolean isLeaf(){
        dataItem.rLock();
        try{
            return getRawIsLeaf(raw);
        }finally {
            dataItem.rUnLock();
        }
    }
    public void release(){
        dataItem.release();
    }

    class InsertAndSplitRes {
        long siblingUid, newSon, newKey;
    }

    class SplitRes {
        long newSon, newKey;
    }

    static void shiftRawKth(SubArray raw, int kth) {
        int begin = raw.start+NODE_HEADER_SIZE+(kth+1)*(8*2);
        int end = raw.start+NODE_SIZE-1;
        for(int i = end; i >= begin; i --) {
            raw.raw[i] = raw.raw[i-(8*2)];
        }
    }


    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception {
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();

        dataItem.before();
        try {
            success = insert(uid, key);
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if(needSplit()) {
                try {
                    SplitRes r = split();
                    res.newSon = r.newSon;
                    res.newKey = r.newKey;
                    return res;
                } catch(Exception e) {
                    err = e;
                    throw e;
                }
            } else {
                return res;
            }
        } finally {
            if(err == null && success) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    private boolean insert(long uid, long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key) {
                kth ++;
            } else {
                break;
            }
        }
        if(kth == noKeys && getRawSibling(raw) != 0) return false;

        if(getRawIfLeaf(raw)) {
            shiftRawKth(raw, kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys+1);
        } else {
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, key, kth);
            shiftRawKth(raw, kth+1);
            setRawKthKey(raw, kk, kth+1);
            setRawKthSon(raw, uid, kth+1);
            setRawNoKeys(raw, noKeys+1);
        }
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }



    private SplitRes split() throws Exception {
        SubArray nodeRaw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(nodeRaw, getRawIfLeaf(raw));
        setRawNoKeys(nodeRaw, BALANCE_NUMBER);
        setRawSibling(nodeRaw, getRawSibling(raw));
        copyRawFromKth(raw, nodeRaw, BALANCE_NUMBER);
        long son = tree.dm.insert(TransactionManagerImpl.SUPER_XID, nodeRaw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newSon = son;
        res.newKey = getRawKthKey(nodeRaw, 0);
        return res;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

}
