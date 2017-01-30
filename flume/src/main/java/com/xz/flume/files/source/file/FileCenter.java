package com.xz.flume.files.source.file;

import com.xz.flume.files.source.task.MetaFileTask;
import com.xz.flume.files.source.utils.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FileCenter {

    private static final Logger logger = LoggerFactory.getLogger(FileCenter.class);
    private static FileCenter fileCenter = new FileCenter();
    private static Map<String, String> globalContext = null;
    private Map<String, FileInfo> map = new HashMap<>();
    private List<FileInfo> list = new ArrayList<>();
    private Lock lock = new ReentrantLock();
    private int index = 0;

    private FileCenter() {

    }

    public static FileCenter newInstance(Map<String, String> globalContext) {
        FileCenter.setGlobalContext(globalContext);
        return fileCenter;
    }

    private static void setGlobalContext(Map<String, String> globalContext) {
        FileCenter.globalContext = globalContext;
    }

    /**
     * 注册文件
     */
    public void registe(File[] files) {
        for (File file : files) {
            String absolutePath = file.getAbsolutePath();
            lock.lock();
            try {
                //内存中是否有这个文件 如果有则已经处理
                if (map.containsKey(absolutePath)) {
                    continue;
                }
                //读取迁移文件 如果迁移文件存在 则存在同名文件 认为处理过 不管
                String moveFilePath = globalContext.get("read") + "/" + globalContext.get("move") + "/" + file.getName();
                File moveFile = new File(moveFilePath);
                if (moveFile.exists()) {
                    if (logger.isDebugEnabled()) {
                        logger.debug(absolutePath + " has been dealed!!");
                    }
                    continue;
                }
                //生成fileInfo对象
                String metaFilePath = globalContext.get("read") + "/" + globalContext.get("meta") + "/" + file.getName();
                MarkInfo markInfo = null;
                if (MetaFileTask.getMarkFilemap().containsKey(absolutePath)) {
                    markInfo = MetaFileTask.getMarkFilemap().get(absolutePath);
                } else {
                    markInfo = new MarkInfo(absolutePath, metaFilePath);
                }
                FileInfo fileInfo = new FileInfo(file.getName(), absolutePath, file.getParent(), file, markInfo);
                System.out.println("deal:" + absolutePath);
                map.put(absolutePath, fileInfo);
                list.add(fileInfo);
                if (logger.isDebugEnabled()) {
                    logger.debug("registe" + absolutePath + "--" + fileInfo.getMarkInfo().toString());
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * 返回fileinfo对象与一行日志 的map集合
     *
     * @return
     */
    public void readLine(List<String> content, int batchNu) {
        lock.lock();
        try {
            if (list.size() < 1) {
                return;
            }
            for (int i = 0; i < batchNu; i++) {
                if (index >= list.size()) {
                    index = 0;
                }
                FileInfo fileInfo = list.get(index);
                content.add(fileInfo.readLine());
                index++;
            }
        } finally {
            lock.unlock();
        }
    }

    public boolean markFile() {
        boolean bo = false;
        lock.lock();
        try {
            for (FileInfo fileInfo : list) {
                fileInfo.mark();
            }
        } finally {
            lock.unlock();
        }
        return bo;
    }

    /**
     * 从全局map中删除fileinfo 并返回删除的fileinfo对象搬迁文件
     *
     * @param timeLimit
     * @param moveParentPath
     */
    public void moveFile(long timeLimit,String moveParentPath) {
        long now = System.currentTimeMillis();
        System.out.println("------move start--------");
        lock.lock();
        try {
            for (FileInfo fileInfo : map.values()) {
                //超时
                if (fileInfo.getState() == FileInfo.FileInfoState.NORMAL_NODATA && now - fileInfo.getFile().lastModified() > timeLimit) {

                    //设置文件状态为关闭 释放连接
                    fileInfo.setState(FileInfo.FileInfoState.CLOSE);
                    fileInfo.release();
                    map.remove(fileInfo.getAbsolutePath());
                    list.remove(fileInfo);

                    File movePathFile = new File(moveParentPath+"/"+fileInfo.getName()) ;
                    boolean bo = FileUtil.move(fileInfo.getFile(),movePathFile);
                    logger.info(fileInfo.getAbsolutePath()+"--move--"+movePathFile.getAbsolutePath()+":"+bo);
                }
            }
        } finally {
            lock.unlock();
        }
        System.out.println("------move end--------");
    }

}
