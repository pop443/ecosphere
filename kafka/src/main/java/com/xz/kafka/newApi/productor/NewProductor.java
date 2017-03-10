package com.xz.kafka.newApi.productor;

import java.util.Properties;

import com.xz.kafka.conf.KafkaConf;
import org.apache.kafka.clients.producer.ProducerConfig;

public class NewProductor {
	public static Properties getConfig() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConf.getBootstrapServers());
		properties.put(ProducerConfig.ACKS_CONFIG, "all");
		properties.put(ProducerConfig.RETRIES_CONFIG, 0);
		properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
		properties.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		return properties;
	}
	public static void main(String[] args) {
		for (int i = 0; i < 2; i++) {
			Runnable r1 = new NewProductorTask(NewProductor.getConfig(), i) ;
			Thread t1 = new Thread(r1) ;
			t1.start();
		}
	}
}
