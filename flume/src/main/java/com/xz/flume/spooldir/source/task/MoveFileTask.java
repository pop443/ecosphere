package com.xz.flume.spooldir.source.task;

import com.xz.flume.spooldir.source.file.FileInfo;
import com.xz.flume.spooldir.source.file.FileCenter;
import com.xz.flume.spooldir.source.utils.FileUtil;
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
    private List<FileInfo> list ;
    private long timeLimit = 1000*10 ;

    public MoveFileTask(String targetPath,FileCenter fileCenter) {
        this.fileCenter = fileCenter;
        this.targetPath = targetPath;
        list = new ArrayList<>() ;

    }

    @Override
    public void run() {
        list.clear();
        fileCenter.moveFile(list,timeLimit);
        for (FileInfo fileInfo:list) {
            String targetPath = this.targetPath+"/"+fileInfo.getName() ;
            File target = new File(targetPath) ;
            boolean bo = FileUtil.move(fileInfo.getFile(),target);
            logger.info(fileInfo.getAbsolutePath()+"--move--"+target.getAbsolutePath()+":"+bo);
        }
    }
}
