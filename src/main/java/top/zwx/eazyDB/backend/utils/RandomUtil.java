package top.zwx.eazyDB.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

/**
 * 生成一个指定长度的、包含安全随机字节的数组
 */
public class RandomUtil {
    public static byte[] randomBytes(int length) {
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
