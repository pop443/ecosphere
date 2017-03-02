package com.xz.hbase.init;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.conf.HbaseConnection;

public class Client {
	public static void main(String[] args) throws IOException {
		Connection connection = HbaseConnection.getHbaseConnection() ;
		boolean bo = Client.createTable(connection) ;
		System.out.println(bo);
		
		connection.close();
	}
	
	private static boolean createTable(Connection connection){
		boolean bo = false ;
		Admin admin = null ;
		try {
			admin = connection.getAdmin() ;
			TableName tableName = TableName.valueOf(HbaseConf.TABLENAME) ;
			
			if (admin.tableExists(tableName)) {
				if (admin.isTableEnabled(tableName)) {
					admin.disableTable(tableName);
				}
				admin.deleteTable(tableName);
				System.out.println("--------重建---------");
			}
			
			HTableDescriptor table = new HTableDescriptor(tableName) ;
			
			HColumnDescriptor family = new HColumnDescriptor(HbaseConf.FAMILYNAME) ;
			table.addFamily(family);
			
			//10000-2000000000000
			//19999-2020000000000
			byte[][] partitions = new byte[5][];
			partitions[0] = Bytes.toBytes("11000-20030505121212");
			partitions[1] = Bytes.toBytes("13000-20070505121212");
			partitions[2] = Bytes.toBytes("15000-20110505121212");
			partitions[3] = Bytes.toBytes("17000-20140505121212");
			partitions[4] = Bytes.toBytes("19000-20180505121212");

			admin.createTable(table, partitions);
			bo = true ;
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if (admin!=null) {
				try {
					admin.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return bo ;
	}
}
