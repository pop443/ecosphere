package com.xz.flume.files.source.task;

import com.xz.flume.filefilter.EndWithFileFilter;
import com.xz.flume.files.source.file.MarkInfo;
import com.xz.flume.files.source.file.FileCenter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * falcon -- 2016/11/25.
 */
public class ScanFolderTask implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(ScanFolderTask.class);
    /** 父文件夹 */
    private File parentFile;
    private FileCenter fileCenter;
    private String fileEnd ;

    public ScanFolderTask(String parentPath, FileCenter fileCenter,String fileEnd) {
        this.parentFile = new File(parentPath);
        this.fileCenter = fileCenter;
        this.fileEnd = fileEnd ;
    }

    @Override
    public void run() {
        if (logger.isDebugEnabled()) {
            logger.debug("--ScanFolderTask start--");
        }
        // 获取文件夹 下的文件
        File[] files = parentFile.listFiles(new EndWithFileFilter(fileEnd)) ;
        //注册文件
        try{
            fileCenter.registe(files);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
