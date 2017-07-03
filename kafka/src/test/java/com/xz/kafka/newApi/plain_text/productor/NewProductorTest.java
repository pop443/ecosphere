package com.xz.kafka.newApi.plain_text.productor;

import com.xz.kafka.conf.KafkaConf;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class NewProductorTest {
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
		for (int i = 0; i < 1; i++) {
			Runnable r1 = new NewProductorTaskTest(NewProductorTest.getConfig(), i) ;
			Thread t1 = new Thread(r1) ;
			t1.start();
		}
	}
}
