package com.xz.hbase.demo1.delete;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.conf.HbaseConnection;
import com.xz.hbase.demo1.put.InsertHugeRowBatchTable;
import com.xz.hbase.util.TableOperation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * falcon -- 2017/2/28.
 */
public class DeleteByRow extends TableOperation {

    private byte[] row ;

    public DeleteByRow(String row) {
        this.row = Bytes.toBytes(row);
    }

    @Override
    protected void doSomeThings() throws IOException {
        Delete delete = new Delete(row) ;
        hTable.delete(delete);
    }

    public static void main(String[] args) {
        String str_row = "11000-20030505121211" ;
        DeleteByRow deleteByRow = new DeleteByRow(str_row) ;
        boolean bo = deleteByRow.handle(TableName.valueOf(HbaseConf.TABLENAME), HbaseConnection.getHbaseConnection()) ;
        System.out.println(bo);
    }
}
