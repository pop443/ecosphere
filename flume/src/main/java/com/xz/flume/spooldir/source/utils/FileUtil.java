package com.xz.flume.spooldir.source.utils;

import org.apache.commons.io.FileUtils;
import org.apache.flume.FlumeException;

import java.io.File;
import java.io.IOException;

/**
 * falcon -- 2016/11/29.
 */
public class FileUtil {

    public static boolean mkdir(File parentfile){
        return parentfile.mkdirs() ;
    }


    public static boolean move(File source,File target){
        boolean bo = false ;
        try {
            FileUtils.moveFile(source,target);
            bo = true ;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bo ;
    }


    public static void main(String[] args) {
        File target = new File("D:\\log\\move\\move") ;
        try {
            FileUtils.forceMkdir(target);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
