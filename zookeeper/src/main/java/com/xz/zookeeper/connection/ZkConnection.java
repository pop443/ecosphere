package com.xz.zookeeper.connection;

import com.xz.zookeeper.conf.ZkConf;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * falcon -- 2016/11/29.
 */
public class ZkConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZkConnection.class);
    private CountDownLatch latch = new CountDownLatch(1);
    private ZooKeeper zk ;

    public ZkConnection() {
    }

    public ZooKeeper getConnection(){
        try {
            zk = new ZooKeeper(ZkConf.getZookeeperip(), ZkConf.getTimeout(), new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                        latch.countDown();
                    }
                }
            });
            latch.await();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return zk ;
    }

    public boolean register(){
        boolean bo = false ;
        try {
            Stat s = zk.exists(ZkConf.getRootnode(), false);
            System.out.println(s);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        return bo ;
    }
    public static void main(String[] args) {
        ZkConnection zkConnection = new ZkConnection() ;
        ZooKeeper zk = zkConnection.getConnection() ;
        boolean bo = zkConnection.register() ;
        System.out.println();
    }
}
