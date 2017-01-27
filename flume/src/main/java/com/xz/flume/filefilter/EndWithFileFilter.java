package com.xz.flume.filefilter;

import java.io.File;
import java.io.FileFilter;

/**
 * falcon -- 2017/1/26.
 * 后缀 过滤 大小写不敏感
 */
public class EndWithFileFilter implements FileFilter{
    private String end ;
    public EndWithFileFilter(String end){
        this.end = end ;
    }
    @Override
    public boolean accept(File file) {
        return !file.isDirectory() && file.getName().toUpperCase().endsWith(end.toUpperCase());
    }
}
