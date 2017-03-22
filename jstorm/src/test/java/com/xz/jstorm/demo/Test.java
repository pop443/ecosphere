package com.xz.jstorm.demo;

import backtype.storm.Config;
import backtype.storm.utils.Utils;

import java.util.Map;

/**
 * falcon -- 2017/3/21.
 */
public class Test {
    public static void main(String[] args) {
        StringBuilder sb = new StringBuilder("word") ;
        sb.reverse();
        System.out.println(sb.toString());
    }
}
