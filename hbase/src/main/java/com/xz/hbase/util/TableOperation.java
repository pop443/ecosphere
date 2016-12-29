package com.xz.hbase.util;

import java.io.IOException;

import com.xz.hbase.conf.HbaseConf;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;

import com.xz.hbase.conf.HbaseConnection;

public abstract class TableOperation {
	protected Connection connection ;
	protected HTable hTable ;
	private boolean canClose = false ;
	
	private void initConnection(TableName name,Connection connection) throws IOException{
		if (connection == null) {
			this.connection = HbaseConnection.getHbaseConnection() ;
			canClose = true ;
		}else{
			this.connection = connection ;
		}
		hTable = (HTable)this.connection.getTable(name) ;
		hTable.setAutoFlushTo(HbaseConf.isCommit);
		hTable.setWriteBufferSize(HbaseConf.writeBufferSize);
	}
	
	public boolean handle(TableName name,Connection connection){
		boolean bo = false ;
		try {
			initConnection(name,connection);
			this.doSomeThings() ;
			bo = commit() ;
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			release();
		}
		return bo ;
	}
	
	public boolean handle(TableName name){
		return this.handle(name, null) ;
	}
	
	protected abstract void doSomeThings()  throws IOException;
	
	private boolean commit() throws IOException{
		boolean bo = false ;
		try {
			if (hTable != null) {
				hTable.flushCommits();
				bo = true ;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			hTable.close();
			hTable = null ;
		}
		return bo ;
	}
	
	private void release(){
		 if (connection!=null && canClose) {
			try {
				connection.close();
				connection = null ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
