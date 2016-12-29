package com.xz.hbase.demo1.put;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.util.TableOperation;

public class InsertHbaseTable extends TableOperation {
	private List<String> list;

	public InsertHbaseTable(List<String> list) {
		this.list = list;
	}
	
	// 18647_20131009231301,1465,1120
	@Override
	public void doSomeThings() {
		List<Put> putList = this.parse(list);
		try {
			super.hTable.put(putList);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private List<Put> parse(List<String> list) {
		List<Put> putList = new ArrayList<>();
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
		return putList;
	}
	
	public static void main(String[] args) {
		List<String> list = new ArrayList<>() ;
		list.add("18647_20131009231301,1465,1120") ;
		TableOperation operation = new InsertHbaseTable(list) ;
		boolean bo = operation.handle(TableName.valueOf(HbaseConf.TABLENAME)) ;
		
	}
}
