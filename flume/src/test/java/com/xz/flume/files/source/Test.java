package com.xz.flume.files.source;

/**
 * falcon -- 2016/11/29.
 */
public class Test {
    public static void main(String[] args) {
        long l = System.currentTimeMillis() ;
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long l2 = System.currentTimeMillis() ;
        System.out.println(l2-l);
    }
}
