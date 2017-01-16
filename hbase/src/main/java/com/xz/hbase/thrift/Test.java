package com.xz.hbase.thrift;

import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;

/**
 * falcon -- 2017/1/16.
 */
public class Test {
    public static void main(String[] args) {
        try {
            TTransport socket = new TSocket("172.32.148.163", 9090);
            TProtocol protocol = new TBinaryProtocol(socket, true, true);// 注意这里
            THBaseService.Iface client = new THBaseService.Client(protocol);
            socket.open();
            ByteBuffer table = ByteBuffer.wrap("kylin_metadata".getBytes());
            TGet get = new TGet();
            get.setRow("/table/DEFAULT.KYLIN_SALES.json".getBytes());
            TResult result = client.get(table, get);
            System.out.println("row = " + new String(result.getRow()));
            for (TColumnValue resultColumnValue : result.getColumnValues()) {
                System.out.println("family = " + new String(resultColumnValue.getFamily()));
                System.out.println("qualifier = " + new String(resultColumnValue.getQualifier()));
                System.out.println("value = " + new String((resultColumnValue.getValue())));
                System.out.println("timestamp = " + resultColumnValue.getTimestamp());
                System.out.println("");
            }
            socket.close();
            System.out.println("close");
        } catch (TException e) {
            e.printStackTrace();
        }


    }
}
