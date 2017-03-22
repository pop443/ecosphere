package com.xz.jstorm.demo.wordcount1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class WordCountTopology {
    public static void main(String[] args)
            throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new RandomSentenceSpout(), 4);

        builder.setBolt("split", new SplitBolt(), 2).localOrShuffleGrouping("spout");
        builder.setBolt("count", new CountBolt(), 2).fieldsGrouping("split", new Fields("word","word2"));

        Config conf = new Config();
        conf.setDebug(true);
        if ((args != null) && (args.length > 0)) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(5000L);
            cluster.shutdown();
        }
    }
}