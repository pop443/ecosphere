package com.xz.jstorm.demo.wordcount1;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

class SplitBolt extends BaseBasicBolt{
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
      }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString(0);
        String[] words = line.split(" ");
        for (String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                word = word.toLowerCase();
                StringBuilder sb = new StringBuilder(word) ;
                sb.reverse() ;
                collector.emit(new Values(word,sb.toString()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","word2"));
    }
}
