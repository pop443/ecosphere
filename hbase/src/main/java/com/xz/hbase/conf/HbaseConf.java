package com.xz.hbase.conf;

import com.xz.common.utils.PropertiesUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;
import java.util.Properties;

public class HbaseConf {
	private static String PATH = "config.properties";
	private static Properties properties = PropertiesUtil.getProperties(PATH);

	public static String TABLENAME_STR = properties.getProperty("table") ;
	public static String FAMILYNAME_STR = properties.getProperty("tablefamily") ;
	public static byte[] TABLENAME = Bytes.toBytes(TABLENAME_STR) ;
	public static byte[] FAMILYNAME = Bytes.toBytes(FAMILYNAME_STR) ;

	public static boolean isCommit = Boolean.parseBoolean(properties.getProperty("commit")) ;
	public static long writeBufferSize = Long.parseLong(properties.getProperty("writebuffersize")) ;
}
