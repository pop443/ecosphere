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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.demo1.vo.OneRow;
import com.xz.hbase.util.TableOperation;
/**
 * 只返回 除了value之外的keyvalue信息
 * @author Administrator
 *
 */
public class KeyOnlyFilterTest extends TableOperation{
	private byte[] startRow = null ;
	private byte[] stopRow = null ;
	
	public KeyOnlyFilterTest(Map<String, String> inMap){
		this.startRow = Bytes.toBytes(inMap.get("startRow")) ;
		this.stopRow = Bytes.toBytes(inMap.get("stopRow")) ;
	}
	@Override
	protected void doSomeThings() throws IOException {
		Scan scan = new Scan() ;
		scan.addFamily(HbaseConf.FAMILYNAME) ;
		boolean isReverse = true ;
		scan.setReversed(isReverse) ;
		if (isReverse) {
			scan.setStartRow(stopRow) ;
			scan.setStopRow(startRow) ;
		}else{
			scan.setStartRow(startRow) ;
			scan.setStopRow(stopRow) ;
		}
		
		scan.setCaching(100);
		scan.setBatch(1);
		Filter filter = new KeyOnlyFilter() ;
		scan.setFilter(filter) ;
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
		Map<String, String> inMap = new HashMap<>() ;
		inMap.put("startRow", "11000-20030505121208") ;
		inMap.put("stopRow", "11000-20030505121210") ;
		KeyOnlyFilterTest hbaseTable = new KeyOnlyFilterTest(inMap) ;
		System.out.println(hbaseTable.handle(TableName.valueOf(HbaseConf.TABLENAME)) );
	}
}
