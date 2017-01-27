package com.xz.flume.files.source;

import com.xz.flume.filefilter.EndWithFileFilter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * falcon -- 2017/1/26.
 */
public class TestFileFilter {
    public static void main(String[] args) throws IOException {
        String path = "E:\\book-go" ;
        File[] files = new File(path).listFiles(new EndWithFileFilter(".h")) ;
        for (File file:files){
            System.out.println(file.getAbsolutePath());
        }
    }
}
