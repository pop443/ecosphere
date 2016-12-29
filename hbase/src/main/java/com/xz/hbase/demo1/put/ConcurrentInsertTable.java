package com.xz.hbase.demo1.put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.exception.ExceptionUtils.Error;
import com.xz.hbase.exception.XZException;

public class ConcurrentInsertTable {

	private Connection connection;
	private HTable hTable;
	private List<Put> putList = new ArrayList<>() ;

	public ConcurrentInsertTable(TableName name, Connection connection)
			throws XZException {
		if (connection == null) {
			throw Error.ConnectionIsNull.wrong();
		}
		try {
			this.connection = connection;
			this.hTable = (HTable) this.connection.getTable(name);
			this.hTable.setAutoFlushTo(HbaseConf.isCommit);
			this.hTable.setWriteBufferSize(HbaseConf.writeBufferSize);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public boolean insert(List<String> strList){
		boolean bo = false ;
		this.parse(strList);
		try {
			hTable.put(putList);
			hTable.flushCommits();
			bo = true ;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bo ;
	}

	private void parse(List<String> list) {
		putList.clear();
		for (String string : list) {
			try {
				String[] strings = string.split(",");
				byte[] rowkey = Bytes.toBytes(strings[0]);
				byte[] family = HbaseConf.FAMILYNAME;
				byte[] qualifier = Bytes.toBytes(strings[1]);
				byte[] value = Bytes.toBytes(strings[2]);

				Put put = new Put(rowkey);
				put.addColumn(family, qualifier, value);
				putList.add(put);
			} catch (Exception e) {
				continue;
			}
		}
	}
	
	public void release() {
		try {
			if (hTable!=null) {
				hTable.close();
				hTable = null ;
			}
			if (connection != null) {
				connection.close();
				connection = null ;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
