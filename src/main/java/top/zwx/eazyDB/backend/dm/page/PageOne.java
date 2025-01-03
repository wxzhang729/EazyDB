package top.zwx.eazyDB.backend.dm.page;

import top.zwx.eazyDB.backend.utils.RandomUtil;

import javax.xml.crypto.Data;
import java.util.Arrays;

/**
 * 用于特殊管理数据的第一页，用于存储元数据，启动检查等
 * ValidCheck
 * db启动时给100~107字节处填入一串随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据是否正常关闭
 */
public class PageOne {
    private static final int LEN_VC = 8;
    private static final int OF_VC = 100;

    /**
     * 启动时设置初始字节
     * @param pg
     */
    public static void setVcOpen(Page pg){
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    private static void setVcOpen(byte[] raw){
        //随机数组、从数组0的位置开始复制、目标数组、在目标数组中的起始偏移量，从源数组中复制LEN_VC个元素
        //这个的意思就是把随机数组的8个字节复制到数据页中，然后偏移量是100，即从100字节开始
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    /**
     * 关闭时拷贝字节
     * @param pg
     */
    public static void setVcClose(Page pg){
        pg.setDirty(true);
        setVcClose(pg.getData());
    }
    private static void setVcClose(byte[] raw){
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    /**
     * 校验字节
     */
    public static boolean checkVc(Page pg){
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw){
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, LEN_VC + OF_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, 2*LEN_VC+OF_VC));
    }
}
