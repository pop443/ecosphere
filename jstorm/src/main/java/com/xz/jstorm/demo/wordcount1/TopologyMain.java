package com.xz.jstorm.demo.wordcount1;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * falcon -- 2017/3/19.
 */
public class TopologyMain {
    public static void main(String[] args) {
        try {
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("WordCountSpout", new WordCountSpout(), 1);
            builder.setBolt("WordCountLineBolt", new WordCountLineBolt(),1).localOrShuffleGrouping("WordCountSpout");
            builder.setBolt("CharCountLineBolt", new CharCountBolt(),2).localOrShuffleGrouping("WordCountLineBolt");
			//随机分组
			builder.setBolt("WordCountWordBolt", new WordCountWordBolt(),2).localOrShuffleGrouping("WordCountLineBolt");
            //按 OutputFieldsDeclarer 里的 fields 分组
           // builder.setBolt("WordCountWordBolt", new WordCountWordBolt(), 2).fieldsGrouping("WordCountLineBolt", new Fields("word"));
            //全量分发
			//builder.setBolt("WordCountWordBolt", new WordCountWordBolt(),2).allGrouping("WordCountLineBolt") ;
            //只选择一个作为处理的bolt
//			builder.setBolt("WordCountWordBolt", new WordCountWordBolt(),2).globalGrouping("WordCountLineBolt") ;


            Config conf = new Config();
            conf.setNumWorkers(1);
            if ((args != null) && (args.length > 0)) {
                System.out.println(1);
                StormSubmitter.submitTopology(args[0], conf,
                        builder.createTopology());
            } else {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("WordCountTopo", conf,
                        builder.createTopology());
                Utils.sleep(100000L);
                cluster.killTopology("WordCountTopo");
                cluster.shutdown();
            }
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        }
    }
}
