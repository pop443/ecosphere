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
        globalContext = new HashMap<>() ;
        ImmutableMap<String, String> map = context.getParameters();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            globalContext.put(key, value);
        }
        fileCenter = FileCenter.newInstance(globalContext) ;

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
