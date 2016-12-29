package com.xz.kafka.oldHighApi.productor;

import com.xz.kafka.conf.KafkaConf;
import kafka.producer.ProducerConfig;

import java.util.Properties;

public class HighProductor {
	public static Properties getConfig() {
		Properties props = new Properties();
		props.put("metadata.broker.list", KafkaConf.getBootstrapServers());
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		return props;
	}
	public static void main(String[] args) {

		ProducerConfig config = new ProducerConfig(HighProductor.getConfig());
		String topic = KafkaConf.getTopic() ;
		for (int i = 0; i < 2; i++) {
			Runnable r1 = new HighProductorTask(config,topic, i) ;
			Thread t1 = new Thread(r1) ;
			t1.start();
		}
	}
}
