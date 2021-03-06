package com.xz.flume.files.source.task;

import com.xz.flume.files.source.file.FileInfo;
import com.xz.flume.files.source.file.FileCenter;
import com.xz.flume.files.source.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * falcon -- 2016/11/29.
 */
public class MoveFileTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(MoveFileTask.class);
    private FileCenter fileCenter ;
    /** 迁移文件的路径 */
    private String targetPath ;
    private long timeLimit = 1000*10 ;

    public MoveFileTask(String targetPath,FileCenter fileCenter) {
        this.fileCenter = fileCenter;
        this.targetPath = targetPath;

    }

    @Override
    public void run() {
        try{
            fileCenter.moveFile(timeLimit,this.targetPath);
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }
}
