package com.xz.flume.files.source.task;

import com.xz.flume.files.source.file.MarkInfo;
import com.xz.flume.files.source.file.FileCenter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * falcon -- 2016/11/26.
 * 由总路径+/meta路径+/mark.xz
 * 只读取一次，之后根据间隔写入
 */
public class MarkFileTask implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(MarkFileTask.class);

    private File file;
    private FileCenter fileCenter ;
    private Map<String, MarkInfo> map ;
    private Pattern pattern ;

    /**
     *
     * @param path -- 总路径+/meta路径
     * @param fileCenter
     */
    public MarkFileTask(String path, FileCenter fileCenter) {
        file = new File(path + "/mark.xz");
        this.fileCenter = fileCenter;
        this.map = new HashMap<>() ;
        this.pattern = Pattern.compile(":") ;

        try {
            if (!file.exists()){
                FileUtils.touch(file);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        readFileMarkInfo();
        if (logger.isDebugEnabled()) {
            logger.debug("Markpath--" + path + "/mark.xz");
        }
    }

    /**
     * 读取持久化文件
     */
    private void readFileMarkInfo() {
        Properties properties = new Properties();
        try {
            InputStream inputStream = new FileInputStream(file);
            properties.load(inputStream);
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String path = (String) entry.getKey();
                String other = (String) entry.getValue();
                String[] infos = pattern.split(other);
                int num = 0;
                long offset = 0;
                try {
                    num = Integer.parseInt(infos[0]);
                    offset = Long.parseLong(infos[1]);
                } catch (NumberFormatException e) {
                    logger.error("init markinfo false--" + e.getMessage());
                }
                MarkInfo info = new MarkInfo(path,num,offset);
                map.put(path, info);
            }
            inputStream.close();
            inputStream = null;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeFileMarkInfo() {
        try {
            String markinfos = fileCenter.getMarkFile();
            FileUtils.write(file, markinfos);
            if (logger.isDebugEnabled()) {
                logger.debug("writeFileMarkInfo--" + markinfos);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Map<String, MarkInfo> getMap() {
        return map;
    }

    @Override
    public void run() {
        writeFileMarkInfo();
    }
}
