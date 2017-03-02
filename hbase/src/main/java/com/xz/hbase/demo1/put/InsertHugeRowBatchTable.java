package com.xz.hbase.demo1.put;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.conf.HbaseConnection;
import com.xz.hbase.util.TableOperation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class InsertHugeRowBatchTable extends TableOperation{

	private String str_row = null ;

	public InsertHugeRowBatchTable(String str_row){
		this.str_row = str_row ;
	}
	
	@Override
	protected void doSomeThings() throws IOException {
		String str_family = "cf1" ;
		
		byte[] row = Bytes.toBytes(str_row) ;
		byte[] family = Bytes.toBytes(str_family) ;
		
		Put put = new Put(row) ;
		for (int i = 0; i < 100000; i++) {
			String str_qualifier = i+"" ;
			String str_value = i+"" ;
			byte[] qualifier = Bytes.toBytes(str_qualifier) ;
			byte[] value = Bytes.toBytes(str_value) ;
			put.addColumn(family, qualifier, value) ;
			if (i%10000==0){
				hTable.put(put);
				put = new Put(row) ;
				System.out.println(i);
			}
		}
		hTable.put(put);
		hTable.flushCommits();
	}
	
	public static void main(String[] args) {
		String str_row = "19000-20030505121211" ;
		InsertHugeRowBatchTable batchTable = new InsertHugeRowBatchTable(str_row) ;
		boolean bo = batchTable.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;
		System.out.println(bo);
	}
}
