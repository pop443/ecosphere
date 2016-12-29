package com.xz.hbase.demo1.scan.useFilter;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.demo1.vo.OneRow;
import com.xz.hbase.util.TableOperation;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * falcon -- 2016/12/9.
 */
public class PageFilterTest extends TableOperation {
    private byte[] startRow = null;
    private byte[] stopRow = null;

    public PageFilterTest(Map<String, String> inMap) {
        this.startRow = Bytes.toBytes(inMap.get("startRow"));
        this.stopRow = Bytes.toBytes(inMap.get("stopRow"));
    }

    @Override
    protected void doSomeThings() throws IOException {
        Scan scan = new Scan();
        scan.addFamily(HbaseConf.FAMILYNAME);
        scan.setCaching(1);
        boolean isReverse = false;
        scan.setReversed(isReverse);
        InclusiveStopFilter inclusiveStopFilter = null;
        if (isReverse) {
            scan.setStartRow(stopRow);
            inclusiveStopFilter = new InclusiveStopFilter(startRow);
        } else {
            scan.setStartRow(startRow);
            inclusiveStopFilter = new InclusiveStopFilter(stopRow);
        }


        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        QualifierFilter qualifierFilter = new QualifierFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator("4".getBytes()));

        PageFilter pageFilter = new PageFilter(2);

        filterList.addFilter(pageFilter);
        filterList.addFilter(inclusiveStopFilter);
        filterList.addFilter(qualifierFilter);
        scan.setFilter(filterList);
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
        inMap.put("startRow", "11000-20030505121203");
        inMap.put("stopRow", "11000-20030505121211");
        PageFilterTest hbaseTable = new PageFilterTest(inMap);
        System.out.println(hbaseTable.handle(TableName
                .valueOf(HbaseConf.TABLENAME)));
    }
}
