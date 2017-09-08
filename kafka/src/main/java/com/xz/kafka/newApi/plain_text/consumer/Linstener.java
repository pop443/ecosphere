package com.xz.kafka.newApi.plain_text.consumer;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by Administrator on 2017-9-8.
 */
public class Linstener implements Runnable {
    private Logger log = Logger.getLogger(Linstener.class);
    private Object o ;
    public List<NewConsumerTask> list = null ;
    public ExecutorService executor ;

    public Linstener(ExecutorService executor){
        o = new Object() ;
        list = new ArrayList<>() ;
        this.executor = executor ;
    }
    public void add(NewConsumerTask r){
        synchronized (o){
            list.add(r) ;
        }
    }
    @Override
    public void run() {
        log.info(" run ");
        synchronized (o){
            for (NewConsumerTask r:list) {
                if (!r.isRun()){
                    executor.submit(r);
                    log.info(" again ");
                }
            }
        }
    }
}
