package com.xz.hbase.demo1.put;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.conf.HbaseConnection;
import com.xz.hbase.util.TableOperation;

public class InsertRowBatchTable extends TableOperation{

	private String str_row = null ;
	
	public InsertRowBatchTable(String str_row){
		this.str_row = str_row ;
	}
	
	@Override
	protected void doSomeThings() throws IOException {
		String str_family = "cf1" ;
		
		byte[] row = Bytes.toBytes(str_row) ;
		byte[] family = Bytes.toBytes(str_family) ;
		
		Put put = new Put(row) ;
		for (int i = 0; i < 20; i++) {
			String str_qualifier = i+"" ;
			String str_value = i+"" ;
			byte[] qualifier = Bytes.toBytes(str_qualifier) ;
			byte[] value = Bytes.toBytes(str_value) ;
			put.addColumn(family, qualifier, value) ;
		}
		hTable.put(put);
	}
	
	public static void main(String[] args) {
		String str_row = "11000-20030505121210" ;
		InsertRowBatchTable batchTable = new InsertRowBatchTable(str_row) ;
		boolean bo = batchTable.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;
		System.out.println(bo);
		
		str_row = "11000-20030505121209" ;
		batchTable = new InsertRowBatchTable(str_row) ;
		bo = batchTable.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;

		str_row = "11000-20030505121208" ;
		batchTable = new InsertRowBatchTable(str_row) ;
		bo = batchTable.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;

		str_row = "11000-20030505121207" ;
		batchTable = new InsertRowBatchTable(str_row) ;
		bo = batchTable.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;

		str_row = "11000-20030505121206" ;
		batchTable = new InsertRowBatchTable(str_row) ;
		bo = batchTable.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;

		str_row = "11000-20030505121205" ;
		batchTable = new InsertRowBatchTable(str_row) ;
		bo = batchTable.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;

		str_row = "11000-20030505121204" ;
		batchTable = new InsertRowBatchTable(str_row) ;
		bo = batchTable.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;

		str_row = "11000-20030505121203" ;
		batchTable = new InsertRowBatchTable(str_row) ;
		bo = batchTable.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;

		System.out.println(bo);
	}

}
