package com.xz.hbase.thrift;

import org.apache.hadoop.hbase.thrift2.generated.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * falcon -- 2017/1/16.
 */
public class Thrift2 {
    public static void main(String[] args) {
        try {
            TTransport socket = new TSocket("172.32.148.165", 9090);
            // THRIFT-601 http 协议访问 thrift 接口 内存溢出bug  启用 framed/compact protocol 协议
            boolean framed = true ;
            if (framed){
                socket = new TFramedTransport(socket);
            }
            TProtocol protocol = new TBinaryProtocol(socket,true,true);// 注意这里
            if (framed){
                protocol = new TCompactProtocol(socket);
            }
            THBaseService.Iface client = new THBaseService.Client(protocol);
            socket.open();
            ByteBuffer table = ByteBuffer.wrap("hbase".getBytes());
            TGet get = new TGet();
            get.setRow("19000-20030505121211".getBytes());
            List<TColumn> list = new ArrayList<>() ;
            TColumn tColumn = new TColumn(ByteBuffer.wrap("cf1".getBytes())) ;
            list.add(tColumn);
            get.setColumns(list) ;
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
