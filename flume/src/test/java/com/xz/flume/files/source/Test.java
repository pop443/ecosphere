package com.xz.flume.files.source;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * falcon -- 2016/11/29.
 */
public class Test {
    public static void main(String[] args) {
        Pattern equalPattern = Pattern.compile("=") ;
        Pattern maohaoPattern = Pattern.compile(":") ;
        String s = "/hadoop/flume/files/2.log=3:16\r\n" ;
        Pattern p = Pattern.compile("\\s*|\t|\r|\n");
        Matcher m = p.matcher(s);
        s = m.replaceAll("");

        System.out.println(s);
        String[] strings = equalPattern.split(s) ;
        String srcPath = strings[0] ;
        long num = Integer.parseInt(maohaoPattern.split(strings[1])[0]);
        long offset = Long.parseLong(maohaoPattern.split(strings[1])[1]) ;
        System.out.println(num);
        System.out.println(offset);
    }
}
