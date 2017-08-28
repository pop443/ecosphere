package com.xz.kafka.conf;

import com.xz.common.utils.PropertiesUtil;

import java.util.Properties;

/**
 * Created by xz on 2016/10/10.
 */
public class KafkaConf {
    //nohup /usr/lib/kafka-0.9.2/bin/kafka-server-start.sh /usr/lib/kafka-0.9.2/com.xz.spark.learn.config/server.properties > /var/log/kafka/kafka_0.9.2.log  &
    //nohup /hadoop/kafka-0.10.0.1/bin/kafka-server-start.sh /hadoop/kafka-0.10.0.1/com.xz.spark.learn.config/server.properties > /hadoop/kafka-0.10.0.1/logs/kafka.log &
    //kafka-topics.sh --create --zookeeper vggapp19:2181 --partitions 4 --replication-factor 1 --topic hbase
    private static String PATH = "config.properties" ;
    private static Properties properties = PropertiesUtil.getProperties(PATH) ;
    private static String topic = properties.getProperty("topic");
    private static String groupId = properties.getProperty("groupid");
    private static String bootstrapServers = properties.getProperty("bootstrapservers");
    private static String zookeeper = properties.getProperty("zookeeper");
    private static String kafkaCommit = properties.getProperty("kafkacommit");

    public static String getTopic() {
        return topic;
    }

    public static String getGroupId() {
        return groupId;
    }

    public static String getBootstrapServers() {
        return bootstrapServers;
    }

    public static String getZookeeper() {
        return zookeeper;
    }

    public static String getKafkaCommit() {
        return kafkaCommit;
    }


}
