package com.xz.jstorm.demo.wordcount2;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * falcon -- 2017/3/19.
 */
public class WordCountLineBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 5892680041066817028L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString(0);

        System.out.println(Thread.currentThread().getName()+line);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(line));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }


}