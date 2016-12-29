package com.xz.hadoop.conf;

import java.util.Map;
import java.util.Properties;

import com.xz.common.utils.PropertiesUtil;

/**
 * Created by xz on 2016/10/9.
 */
public class HadoopConf {
    private static String PATH = "config.properties";

    private static Properties properties = PropertiesUtil.getProperties(PATH);

    private static String hdfsPrefixpath = properties.getProperty("fs.defaultFS");

    private static String mr2DemoPrefixpath = hdfsPrefixpath + properties.getProperty("MRDemoPath");

    private static String hiveDemoPrefixpath = hdfsPrefixpath + properties.getProperty("HIVEDemoPath");

    public static Properties getProperties() {
        return properties;
    }

    public static String getHdfsPrefixpath() {
        return hdfsPrefixpath;
    }

    public static String getMr2DemoPrefixpath() {
        return mr2DemoPrefixpath;
    }
    public static String getHiveDemoPrefixpath() {
        return hiveDemoPrefixpath;
    }

}
