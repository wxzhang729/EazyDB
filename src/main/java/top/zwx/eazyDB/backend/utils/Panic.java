package top.zwx.eazyDB.backend.utils;

public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();      //将错误日志输出到控制台
        System.exit(1);         //终止进程
    }
}
