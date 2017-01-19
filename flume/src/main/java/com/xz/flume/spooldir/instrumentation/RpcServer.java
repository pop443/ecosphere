package com.xz.flume.spooldir.instrumentation;

import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * falcon -- 2017/1/19.
 */
public class RpcServer implements MonitorService {
    private String zookeeperIP = null ;
    private ScheduledExecutorService service = null ;
    @Override
    public void start() {
        service.scheduleWithFixedDelay(null,0,30, TimeUnit.SECONDS) ;

    }

    @Override
    public void stop() {

    }

    @Override
    public void configure(Context context) {
        zookeeperIP = context.getString("zookeeperIP") ;
        service = Executors.newSingleThreadScheduledExecutor() ;
    }
}
