package com.xz.hadoop.util;

import org.apache.hadoop.fs.GlobPattern;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * falcon -- 2017/3/30.
 */
public class TestFilter {
    public static void main(String[] args) {
        String s = "container.ContainerResource_container124234545" ;
        String reg = "^(container.ContainerResource_container.*)" ;
        Pattern pattern = Pattern.compile(reg) ;
        //Pattern pattern = GlobPattern.compile(reg) ;
        Matcher matcher = pattern.matcher(s) ;
        System.out.println(matcher.matches());

    }
}
