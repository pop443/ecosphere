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
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.demo1.vo.OneRow;
import com.xz.hbase.util.TableOperation;

/**
 * 用一列的值确定一行是否被过滤  相当于where 字句
 * setFilterIfMissing 代表 如果这列不存在 是否过滤掉  true过滤掉 false不过滤
 * @author xz
 *
 */
public class SingleColumnValueFilterTest extends TableOperation{
	private byte[] startRow = null;
	private byte[] stopRow = null;

	public SingleColumnValueFilterTest(Map<String, String> inMap) {
		this.startRow = Bytes.toBytes(inMap.get("startRow"));
		this.stopRow = Bytes.toBytes(inMap.get("stopRow"));
	}
	@Override
	protected void doSomeThings() throws IOException {
		Scan scan = new Scan() ;
		
		scan.setStartRow(startRow) ;
		scan.setStopRow(stopRow) ;
		SingleColumnValueFilter filter = new SingleColumnValueFilter(HbaseConf.FAMILYNAME, Bytes.toBytes("15"), CompareOp.NOT_EQUAL, Bytes.toBytes("2")) ;
		filter.setFilterIfMissing(true);
		filter.setLatestVersionOnly(true);
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
		SingleColumnValueFilterTest hbaseTable = new SingleColumnValueFilterTest(inMap);
		System.out.println(hbaseTable.handle(TableName
				.valueOf(HbaseConf.TABLENAME)));
	}

}
