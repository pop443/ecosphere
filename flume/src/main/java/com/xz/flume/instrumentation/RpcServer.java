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
    private String zookeeper = null ;
    private ScheduledExecutorService service = null ;
    @Override
    public void start() {
        service.scheduleWithFixedDelay(this,0,10, TimeUnit.SECONDS) ;
    }

    @Override
    public void stop() {
        service.shutdown();
    }

    @Override
    public void configure(Context context) {
        zookeeper = context.getString("zookeeper") ;
        service = Executors.newSingleThreadScheduledExecutor() ;

    }

    @Override
    public void run() {
        System.out.println("--------------rpc--------------");
        System.out.println(zookeeper);
        Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
        String all = getAll(metricsMap) ;
        System.out.println(all);
        System.out.println("--------------rpc--------------");
    }
    private String getAll(Map<String, Map<String, String>> metricsMap){
        StringBuilder sb = new StringBuilder() ;
        for (Map.Entry<String,Map<String,String>> entry:metricsMap.entrySet()){
            sb.append(entry.getKey()).append("{");
            for (Map.Entry<String,String> entry2:entry.getValue().entrySet()){
                sb.append(entry2.getKey()).append(":").append(entry2.getValue()).append(",");
            }
            sb.append("},");
        }
        return sb.toString() ;
    }
}
