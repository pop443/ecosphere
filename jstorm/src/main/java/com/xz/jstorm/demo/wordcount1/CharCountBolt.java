package com.xz.jstorm.demo.wordcount1;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.*;

/**
 * falcon -- 2017/3/19.
 */
public class CharCountBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 2659767638736897970L;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector arg1) {
        String line = input.getString(0) ;
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