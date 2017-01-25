package com.xz.flume.files.source.task;

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

    private File parentFile;
    private FileCenter fileCenter;
    private Map<String, MarkInfo> markMap ;

    public ScanFolderTask(String parentPath, FileCenter fileCenter, Map<String, MarkInfo> markMap) {
        this.parentFile = new File(parentPath);
        this.fileCenter = fileCenter;
        this.markMap = markMap;
    }

    @Override
    public void run() {
        if (logger.isDebugEnabled()) {
            logger.debug("--ScanFolderTask start--");
        }
        // 获取文件夹 下的文件
        IOFileFilter fileFilter = FileFilterUtils.trueFileFilter();
        List<File> list = (List<File>) FileUtils.listFiles(parentFile, fileFilter,null) ;
        //注册文件
        fileCenter.registe(list,markMap);
    }
}
