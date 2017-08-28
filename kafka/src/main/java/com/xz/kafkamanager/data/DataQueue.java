package com.xz.kafkamanager.data;

/**
 * Created by Administrator on 2017-7-19.
 */
public class DataQueue {
    private DataQueue(){}

    private static class DateQueueBuilder{
        private static DataQueue dataQueue = new DataQueue() ;

    }
    public static final DataQueue getInstance() {
        return DateQueueBuilder.dataQueue;
    }

    public static void main(String[] args) {
        DataQueue dataQueue1 = DataQueue.getInstance() ;
        DataQueue dataQueue2 = DataQueue.getInstance() ;
        DataQueue dataQueue3 = DataQueue.getInstance() ;
        System.out.println(dataQueue1==dataQueue2);
        System.out.println(dataQueue1==dataQueue3);
    }
}
