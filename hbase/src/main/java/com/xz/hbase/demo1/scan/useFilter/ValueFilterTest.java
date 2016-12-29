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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.demo1.vo.OneRow;
import com.xz.hbase.util.TableOperation;
/**
 * 不针对具体列 只过滤列值的过滤器
 * @author Administrator
 *
 */
public class ValueFilterTest extends TableOperation{
	private byte[] startRow = null;
	private byte[] stopRow = null;

	public ValueFilterTest(Map<String, String> inMap) {
		this.startRow = Bytes.toBytes(inMap.get("startRow"));
		this.stopRow = Bytes.toBytes(inMap.get("stopRow"));
	}
	@Override
	protected void doSomeThings() throws IOException {
		Scan scan = new Scan();
		scan.setStartRow(startRow);
		scan.setStopRow(stopRow) ;
		Filter filter = new ValueFilter(CompareOp.EQUAL, new SubstringComparator("16")) ;
		scan.setFilter(filter) ;
		
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
		ValueFilterTest hbaseTable = new ValueFilterTest(inMap);
		System.out.println(hbaseTable.handle(TableName
				.valueOf(HbaseConf.TABLENAME)));
	}

}
