package com.xz.flume.files.source.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * falcon -- 2017/2/1.
 */
public class StringUtil {

    /**
     * 去除字符串中的空格、回车、换行符、制表符
     * @param str
     * @return
     */
    public static String replaceBlank(String str) {
        String dest = "";
        if (str!=null) {
            Pattern p = Pattern.compile("\\s*|\t|\r|\n");
            Matcher m = p.matcher(str);
            dest = m.replaceAll("");
        }
        return dest;
    }
}
