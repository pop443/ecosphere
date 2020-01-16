package com.xz.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by Administrator on 2020/1/16.
 */
public class Operation {
    public static void main(String[] args) {
        try {
            final CountDownLatch countDownLatch = new CountDownLatch(1);

            ZooKeeper zk = new ZooKeeper("172.32.148.244:2181", 3000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        countDownLatch.countDown();
                    }
                    System.out.println("Watch =>" + event.getType());
                }
            });
            countDownLatch.await();

            System.out.println(zk.getState());
            String node = "/xzIgnite/n";


            Stat state = new Stat();
            byte[] bytes = zk.getData(node, false, state) ;

            System.out.println("获取data值 =》" + new String(bytes));


            zk.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

}
