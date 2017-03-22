package com.xz.jstorm.demo.drpc;

import backtype.storm.Config;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import shade.storm.org.apache.thrift.TException;
import shade.storm.org.apache.thrift.transport.TTransportException;

/**
 * falcon -- 2017/3/22.
 */
public class Main {
    public static void main(String[] args) {
        Config config = new Config() ;
        try {
            DRPCClient drpcClient = new DRPCClient(config,"172.32.149.128",3772) ;
            String s = drpcClient.execute("jstorm","list") ;
            System.out.println(s);
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (DRPCExecutionException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        } catch (TException e) {
            e.printStackTrace();
        }
    }
}
