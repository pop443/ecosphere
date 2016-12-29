package com.xz.hbase.demo1.scan.useFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.InclusiveStopFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.demo1.vo.OneRow;
import com.xz.hbase.util.TableOperation;
/**
 * scan 范围[start,stop)
 * 使用 scan 设置start InclusiveStopFilter设置stop 达到[start,stop]效果
 * @author xz
 *
 */
public class InclusiveStopFilterTest extends TableOperation{
	private byte[] startRow = null ;
	private byte[] stopRow = null ;
	
	public InclusiveStopFilterTest(Map<String, String> inMap){
		this.startRow = Bytes.toBytes(inMap.get("startRow")) ;
		this.stopRow = Bytes.toBytes(inMap.get("stopRow")) ;
	}
	@Override
	protected void doSomeThings() throws IOException {
		Scan scan = new Scan() ;
		scan.setStartRow(startRow) ;
		InclusiveStopFilter filter = new InclusiveStopFilter(stopRow) ;
		scan.setFilter(filter) ;
		scan.setBatch(1);
		scan.setCaching(100);
		
		ResultScanner resultScanner = hTable.getScanner(scan) ;
		Result result = null ;
		while ((result = resultScanner.next())!=null) {
			System.out.println(result);
			List<Cell> list = result.listCells() ;
			for (int i = 0; i <list.size(); i++) {
				Cell cell = list.get(i) ;
				OneRow oneRow = new OneRow(cell) ;
				System.out.println(oneRow);
			}
		}
		
	}
	public static void main(String[] args) {
		Map<String, String> inMap = new HashMap<String, String>() ;
		inMap.put("startRow", "11000-20030505121209") ;
		inMap.put("stopRow", "11000-20030505121210") ;
		InclusiveStopFilterTest hbaseTable = new InclusiveStopFilterTest(inMap) ;
		System.out.println(hbaseTable.handle(TableName.valueOf(HbaseConf.TABLENAME)) );
	}
}
