package com.xz.kafka.partitioner;

import kafka.producer.Partitioner;

/**
 * falcon -- 2016/12/29.
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(Object key, int num) {
        return Math.abs(key.hashCode()) % num ;
    }
}
