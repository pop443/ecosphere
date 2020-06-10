package com.xz.kafka.partitioner;


/**
 * falcon -- 2016/12/29.
 */
public class MyPartitioner  {

    public int partition(Object key, int num) {
        return Math.abs(key.hashCode()) % num ;
    }
}
