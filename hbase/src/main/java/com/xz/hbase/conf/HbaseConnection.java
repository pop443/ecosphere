package com.xz.hbase.conf;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import com.xz.hadoop.mr2.util.MRUtil;

public class HbaseConnection {
	private static Configuration hConfiguration = null;
	static{
		hConfiguration = HBaseConfiguration.create();
	}
	
	public static Connection getHbaseConnection(){
		Connection connection = null ;
		try {
			connection = ConnectionFactory.createConnection(hConfiguration);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return connection ;
	}
	
	public static void releaseConnection(Connection connection){
		if (connection!=null && !connection.isClosed()) {
			try {
				connection.close();
				connection = null ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	

	public static void main(String[] args) {
	}
}
