package com.xz.flume.files.source.plat;

import com.google.common.collect.ImmutableMap;
import com.xz.flume.files.source.file.FileCenter;
import org.apache.flume.Context;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public abstract class BaseSource extends AbstractSource implements Configurable, PollableSource {

    protected ScheduledExecutorService executor;
    protected FileCenter fileCenter ;
    protected Map<String,String> globalContext ;

    @Override
    public void configure(Context context) {
        executor = Executors.newScheduledThreadPool(5);
        fileCenter = FileCenter.newInstance() ;
        globalContext = new HashMap<>() ;
        ImmutableMap<String, String> map = context.getParameters();
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            String key = entry.getKey();
            String value = entry.getValue();
            globalContext.put(key,value) ;
        }

    }

    @Override
    public synchronized void stop() {
        super.stop();
        executor.shutdown();
        try {
            this.executor.awaitTermination(10L, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            System.out.println("线程池没有关闭 ");
        }
        executor.shutdownNow();
    }
}
