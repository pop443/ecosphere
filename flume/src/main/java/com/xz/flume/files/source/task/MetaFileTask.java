package com.xz.flume.files.source.task;

import com.xz.flume.filefilter.EndWithFileFilter;
import com.xz.flume.files.source.file.FileCenter;
import com.xz.flume.files.source.file.MarkInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * falcon -- 2016/11/26.
 * 由总路径+/meta路径+/mark.xz
 * 只读取一次，之后根据间隔写入
 */
public class MetaFileTask implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(MetaFileTask.class);
    private String metaParentPath;
    private FileCenter fileCenter ;
    /** 内存中的持久化数据 */
    private static Map<String, MarkInfo> markFilemap = new HashMap<>() ;

    /**
     *
     * @param path -- 总路径+/meta路径
     * @param fileCenter
     */
    public MetaFileTask(String path, FileCenter fileCenter) {
        metaParentPath = path ;
        this.fileCenter = fileCenter;
        readFileMarkInfo();
    }

    /**
     * 读取持久化 .meta 文件
     */
    private void readFileMarkInfo() {
        File metaFile = new File(metaParentPath) ;
        File[] files = metaFile.listFiles(new EndWithFileFilter(".meta")) ;
        for (File file:files){
            MarkInfo markInfo = new MarkInfo(file) ;
            markFilemap.put(markInfo.getSrcPath(),markInfo) ;
        }
    }

    private void writeFileMarkInfo() {
        fileCenter.markFile();
    }

    public static Map<String, MarkInfo> getMarkFilemap() {
        return markFilemap;
    }

    @Override
    public void run() {
        try{
            writeFileMarkInfo();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
