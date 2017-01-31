package com.xz.flume.instrumentation;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.Context;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.util.JMXPollUtil;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * falcon -- 2017/1/19.
 */
public class RpcServer implements MonitorService,Runnable {
    private String zookeeper = null ;
    private ScheduledExecutorService service = null ;
    private ZooKeeper zk = null ;
    private CountDownLatch latch = null;
    private String root = "/ShellZn" ;
    private List<String> ipList = null ;
    private Lock lock = null ;
    private Random random = null ;
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
        lock = new ReentrantLock();
        random = new Random() ;
        zookeeper = context.getString("zookeeper") ;
        service = Executors.newSingleThreadScheduledExecutor() ;
        latch = new CountDownLatch(1);
        try {
            zk = new ZooKeeper(zookeeper,6000,new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                    if (event.getType() == Event.EventType.NodeChildrenChanged
                            && root.equals(event.getPath())){
                        updateServer();
                    }
                }
            }) ;
            latch.await();
            if (zk!=null && zk.getState()== ZooKeeper.States.CONNECTED){
                try {
                    ipList = zk.getChildren(root,true) ;
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void updateServer() {
        lock.lock();
        try{
            if (zk!=null && zk.getState()== ZooKeeper.States.CONNECTED){
                ipList = zk.getChildren(root,true);
                System.out.println("通过回调获得子节点");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        System.out.println("--------------rpc start--------------");
        Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
        String all = getAll(metricsMap) ;
        lock.lock();
        try {
            if (ipList.size()>0){
                int i = random.nextInt(ipList.size()) ;
                String zn = ipList.get(i) ;
                String znPath = root+"/"+zn ;
                String data = new String(zk.getData(znPath,false, null),"utf-8");
                System.out.println(data);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        System.out.println("--------------rpc end--------------");
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
