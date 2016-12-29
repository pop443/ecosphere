package com.xz.zookeeper.conf;

import com.xz.common.utils.PropertiesUtil;

import java.util.Properties;

/**
 * falcon -- 2016/11/29.
 */
public class ZkConf {
    private static String PATH = "zookeeper.properties" ;
    private static Properties properties = PropertiesUtil.getProperties(PATH) ;
    private static String zookeeperip = properties.getProperty("zookeeperip") ;
    private static int timeout = Integer.parseInt(properties.getProperty("timeout")) ;
    private static String rootnode = "/"+properties.getProperty("rootnode") ;
    private static String datanode = rootnode + "/" + properties.getProperty("datanode") ;

    public static String getZookeeperip() {
        return zookeeperip;
    }

    public static int getTimeout() {
        return timeout;
    }

    public static String getRootnode() {
        return rootnode;
    }

    public static String getDatanode() {
        return datanode;
    }
}
