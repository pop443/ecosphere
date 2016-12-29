package com.xz.hadoop.mr2.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

import com.xz.hadoop.conf.HadoopConf;

public class MRUtil {
	
	public static Configuration getConfiguration(Map<String, String> inMap) {
		Configuration conf = new Configuration();
		init(conf) ;
		if (inMap != null) {
			Iterator<Entry<String, String>> it = inMap.entrySet().iterator();
			while (it.hasNext()) {
				Entry<String, String> entry = it.next();
				String key = entry.getKey();
				String value = entry.getValue();
				conf.set(key, value);
			}
		}
		return conf;
	}
	
	private static void init(Configuration conf) {
		Iterator<Entry<Object, Object>> it = HadoopConf.getProperties().entrySet().iterator();
		while (it.hasNext()) {
			Entry<Object, Object> entry = it.next();
			String key = (String)entry.getKey();
			String value = (String)entry.getValue();
			conf.set(key, value);
		}
	}

}
