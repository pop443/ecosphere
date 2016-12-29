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
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.demo1.vo.OneRow;
import com.xz.hbase.util.TableOperation;

/**
 * 针对rowkey的过滤
 * 
 * @author Administrator
 *
 */
public class RowFilterTest extends TableOperation {
	private byte[] startRow = null;
	private byte[] stopRow = null;

	public RowFilterTest(Map<String, String> inMap) {
		this.startRow = Bytes.toBytes(inMap.get("startRow"));
		this.stopRow = Bytes.toBytes(inMap.get("stopRow"));
	}

	@Override
	protected void doSomeThings() throws IOException {
		Scan scan = new Scan();

		// demo1 大小关系
		// 取 rowkey <= 11000-20030505121210 且列名为3 的结果
		// byte[] rowKey = Bytes.toBytes("11000-20030505121210") ;
		// scan.setStartRow(startRow);
		// scan.setStopRow(stopRow) ;
		// scan.addColumn(HbaseConf.FAMILYNAME,Bytes.toBytes("3")) ;
		// Filter filter = new RowFilter(CompareOp.LESS_OR_EQUAL, new
		// BinaryComparator(rowKey)) ;
		// scan.setFilter(filter) ;

		// demo2 前缀
		// 取 rowkey like '10000_2001%'
		// byte[] rowKey = Bytes.toBytes("10000_2001") ;
		// Filter filter = new RowFilter(CompareOp.EQUAL, new
		// BinaryPrefixComparator(rowKey)) ;
		// scan.setFilter(filter) ;

		// demo3 包含
		// 取 rowkey like '%2001%'
		// Filter filter = new RowFilter(CompareOp.EQUAL,
		// new SubstringComparator("_2001"));
		// scan.setFilter(filter);

		// demo4 正则
		// 取 rowkey like '%58'
		// Filter filter = new RowFilter(CompareOp.EQUAL, new
		// RegexStringComparator(".*58$"));
		// scan.setFilter(filter);

		ResultScanner resultScanner = hTable.getScanner(scan);
		Result result = null;
		while ((result = resultScanner.next()) != null) {
			System.out.println(result);
			List<Cell> list = result.listCells();
			for (int i = 0; i < list.size(); i++) {
				Cell cell = list.get(i);
				OneRow oneRow = new OneRow(cell);
				System.out.println(oneRow);
			}
		}
	}

	public static void main(String[] args) {
		Map<String, String> inMap = new HashMap<>();
		inMap.put("startRow", "11000-20030505121208");
		inMap.put("stopRow", "11000-20030505121211");
		RowFilterTest hbaseTable = new RowFilterTest(inMap);
		System.out.println(hbaseTable.handle(TableName
				.valueOf(HbaseConf.TABLENAME)));
	}
}
