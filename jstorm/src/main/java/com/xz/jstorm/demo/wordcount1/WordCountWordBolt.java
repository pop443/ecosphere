package com.xz.jstorm.demo.wordcount1;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * falcon -- 2017/3/19.
 */
public class WordCountWordBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -1856555853843305970L;


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString(0);
        System.out.println(Thread.currentThread().getName()+line);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
    }

}
