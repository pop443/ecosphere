package com.xz.hbase.demo1.vo;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class OneRow {
	private byte[] row;
	private byte[] family;
	private byte[] qualifier;
	private byte[] value;

	public OneRow(Cell cell) {
		row = CellUtil.cloneRow(cell);
		family = CellUtil.cloneFamily(cell);
		qualifier = CellUtil.cloneQualifier(cell);
		value = CellUtil.cloneValue(cell);

	}

	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("OneRow{")
			.append("row:").append(Bytes.toString(row)).append(";")
			.append("family:").append(Bytes.toString(family)).append(";")
			.append("qualifier:").append(Bytes.toString(qualifier)).append(";")
			.append("value:").append(Bytes.toString(value)).append(";")
			.append("}");
		return sb.toString();
	}

}
