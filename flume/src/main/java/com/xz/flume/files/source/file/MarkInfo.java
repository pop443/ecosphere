package com.xz.flume.files.source.file;

import com.xz.flume.files.source.utils.FileUtil;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

/**
 * falcon -- 2016/11/26.
 * 文件元数据信息
 */
public class MarkInfo {
    private static Pattern equalPattern = Pattern.compile("=") ;
    private static Pattern maohaoPattern = Pattern.compile(":") ;

    /** 源文件路径 */
    private String srcPath ;
    /** 源文件路径 */
    private String targetPath ;
    private File targetfile ;
    /** 行数 */
    private AtomicInteger num;
    /** 偏移量 */
    private AtomicLong offset;

    public MarkInfo(File targetfile) {
        try {
            String s = FileUtils.readFileToString(targetfile) ;
            String[] strings = equalPattern.split(s) ;
            srcPath = strings[0] ;
            targetPath = targetfile.getAbsolutePath() ;
            this.targetfile = targetfile ;
            num = new AtomicInteger(Integer.parseInt(maohaoPattern.split(strings[1])[0]));
            offset = new AtomicLong(Long.parseLong(maohaoPattern.split(strings[1])[1])) ;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public MarkInfo(String srcPath,String targetPath) {
        this(srcPath,targetPath,0,0) ;
    }

    public MarkInfo(String srcPath,String targetPath, int num, long offset) {
        this.srcPath = srcPath;
        this.targetPath = targetPath;
        targetfile = new File(this.targetPath) ;
        this.num = new AtomicInteger(num);
        this.offset = new AtomicLong(offset);
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
        sb.append(srcPath).append("=").append(num).append(":").append(offset);
        return sb.toString();
    }

    public String getSrcPath() {
        return srcPath;
    }

    public boolean mark()  {
        boolean bo = false ;
        try {
            FileUtils.write(targetfile,this.toString());
            bo = true ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bo ;
    }
}
