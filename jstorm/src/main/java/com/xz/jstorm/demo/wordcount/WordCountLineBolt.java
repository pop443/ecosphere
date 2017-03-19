package com.xz.jstorm.demo.wordcount;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import shade.storm.org.apache.commons.lang.StringUtils;

/**
 * falcon -- 2017/3/19.
 */
public class WordCountLineBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 5892680041066817028L;

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String line = input.getString(0);
        String[] words = line.split(" ");
        for (int i = 0; i < words.length; i++) {
            String word = words[i].trim();
            if (StringUtils.isNotBlank(word)) {
                collector.emit(new Values(word.toLowerCase()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }


}