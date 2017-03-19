package com.xz.jstorm.demo.wordcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * falcon -- 2017/3/19.
 */
public class WordCountWordBolt extends BaseBasicBolt {
    private static final long serialVersionUID = -1856555853843305970L;
    private Map<String, List<Integer>> map = new HashMap<>();


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        Timer timer = new Timer();
        TimerTask timerTask = new CountsTask(map);
        timer.schedule(timerTask, 1000l, 10000l);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getString(0);
        if (map.containsKey(word)) {
            map.get(word).add(1);
        } else {
            List<Integer> list = new ArrayList<>();
            list.add(1);
            map.put(word, list);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer arg0) {
    }

    public class CountsTask extends TimerTask {
        Map<String, List<Integer>> map = null;

        public CountsTask(Map<String, List<Integer>> map) {
            this.map = map;
        }

        @Override
        public void run() {
            Iterator<String> it = map.keySet().iterator();
            System.out.println("-------word----------" + map.hashCode() + "-------------------");
            while (it.hasNext()) {
                String key = it.next();
                List<Integer> list = map.get(key);
                String value = list.size() + "";
                System.out.println("---key:" + key + ",count:" + value + "---");
            }
            System.out.println("------------------------------------");
        }
    }
}
