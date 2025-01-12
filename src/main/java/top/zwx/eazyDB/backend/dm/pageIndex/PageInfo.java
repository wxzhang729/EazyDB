package top.zwx.eazyDB.backend.dm.pageIndex;

/**
 * 包含页号和空闲空间大小的信息
 */
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
