package com.xz.flume.files.source.file;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * falcon -- 2016/11/26.
 * 文件元数据信息
 */
public class MarkInfo {
    /** 路径 */
    private String path ;
    /** 行数 */
    private AtomicInteger num;
    /** 偏移量 */
    private AtomicLong offset;

    public MarkInfo(String path) {
        this(path,0,0) ;
    }

    public MarkInfo(String path, int num, long offset) {
        this.path = path;
        this.num = new AtomicInteger(num);
        this.offset = new AtomicLong(offset);
    }

    public String getPath() {
        return path;
    }

    public int getNum() {
        return num.get();
    }

    public long getOffset() {
        return offset.get();
    }

    public void autoIncrement(long offset){
        num.incrementAndGet() ;
        this.offset.addAndGet(offset+1) ;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(path).append("=").append(num).append(":").append(offset);
        return sb.toString();
    }
}
