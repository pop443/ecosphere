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
import org.apache.zookeeper.data.Stat;

import java.io.*;
import java.lang.reflect.Type;
import java.net.*;
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
                    //如果不存在根节点 不添加定时任务
                    Stat stat = zk.exists(root,false) ;
                    if (stat==null){
                        return ;
                    }
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
        service.scheduleWithFixedDelay(this,0,10, TimeUnit.SECONDS) ;
    }

    @Override
    public void stop() {
        if(service!=null){
            service.shutdown();
        }
    }

    @Override
    public void configure(Context context) {
        lock = new ReentrantLock();
        random = new Random() ;
        zookeeper = context.getString("zookeeper") ;
        service = Executors.newSingleThreadScheduledExecutor() ;
        latch = new CountDownLatch(1);
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
        lock.lock();
        try {
            if (ipList==null||ipList.size()==0){
                return ;
            }
            int i = random.nextInt(ipList.size()) ;
            String zn = ipList.get(i) ;
            String znPath = root+"/"+zn ;
            String znData = new String(zk.getData(znPath,false, null),"utf-8");
            System.out.println(znData);
            sendData(znData);

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

    private void sendData(String znData) {
        try {

            URL url = new URL("http://"+znData);
            HttpURLConnection servletConnection = (HttpURLConnection) url
                    .openConnection();
            // 设置连接参数
            servletConnection.setRequestMethod("POST");
            servletConnection.setDoOutput(true);
            servletConnection.setDoInput(true);
            servletConnection.setAllowUserInteraction(true);

            // 开启流，写入XML数据
            OutputStream output = servletConnection.getOutputStream();
            Map<String, Map<String, String>> metricsMap = JMXPollUtil.getAllMBeans();
            String all = getAll(metricsMap) ;
            StringBuilder sendStr = new StringBuilder() ;
            sendStr.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
            sendStr.append("<root>");
            sendStr.append(" <ip>").append(InetAddress.getLocalHost().getHostAddress()).append(" </ip>");
            sendStr.append(" <type>").append("HTTP").append(" </type>");
            sendStr.append(" <date>").append(System.currentTimeMillis()).append(" </date>");
            sendStr.append(" <content>").append(all).append(" </content>");
            sendStr.append("</root>");
            System.out.println(sendStr.toString());

            output.write(sendStr.toString().getBytes());
            output.flush();
            output.close();

            // 获取返回的数据
            InputStream inputStream = servletConnection.getInputStream();
            String strMessage = "";
            StringBuffer buffer = new StringBuffer();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            while ((strMessage = reader.readLine()) != null) {
                buffer.append(strMessage);
            }

            System.out.println("接收返回值:" + buffer);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (ProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
