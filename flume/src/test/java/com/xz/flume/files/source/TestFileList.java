package com.xz.flume.files.source;

import java.io.File;
import java.util.List;

/**
 * falcon -- 2017/1/27.
 */
public class TestFileList {
    public static void main(String[] args) {
        File file = new File("E:\\book-go\\_EasyCHM_ErrorLog.Log") ;
        String[] strings = file.list() ;
        for (String string:strings){
            System.out.println(string);
        }
    }
}
