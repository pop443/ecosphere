package com.xz.kafka.oldHighApi.consumer;

import com.xz.kafka.conf.KafkaConf;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HighConsumer {
    public static Properties getConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaConf.getZookeeper());
        // smallest largest anything else none
        props.put("auto.offset.reset", "smallest");
        props.put("group.id", KafkaConf.getGroupId());
        props.put("enable.auto.commit", KafkaConf.getKafkaCommit());
        props.put("auto.commit.enable", "true");
        props.put("auto.commit.interval.ms", "60000");
        return props;
    }

    public static void main(String[] args) {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(HighConsumer.getConfig()));
        int num = 2;
        String topic = "testreset2";
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, new Integer(num));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> steams = consumerMap.get(topic);

        ExecutorService executorService = Executors.newFixedThreadPool(num);
        for (int i = 0; i < steams.size(); i++) {
            executorService.submit(new HighConsumerTask(steams.get(i), i));
        }
    }
}
