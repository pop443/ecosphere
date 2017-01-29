package com.xz.flume.instrumentation;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * falcon -- 2017/1/19.
 */
public class RpcServer implements MonitorService,Runnable {
    private String zookeeperIP = null ;
    private ScheduledExecutorService service = null ;
    @Override
    public void start() {
        service.scheduleWithFixedDelay(this,0,30, TimeUnit.SECONDS) ;
    }

    @Override
    public void stop() {
        service.shutdown();
    }

    @Override
    public void configure(Context context) {
        zookeeperIP = context.getString("zookeeperIP") ;
        service = Executors.newSingleThreadScheduledExecutor() ;

    }

    @Override
    public void run() {
        System.out.println("--------------rpc--------------");
        System.out.println(zookeeperIP);
        Map metricsMap = JMXPollUtil.getAllMBeans();
        Gson gson = new Gson();
        Type mapType = new TypeToken(){
        }.getType();
        String json = gson.toJson(metricsMap, mapType);
        System.out.println(json);
        System.out.println("--------------rpc--------------");
    }
}
