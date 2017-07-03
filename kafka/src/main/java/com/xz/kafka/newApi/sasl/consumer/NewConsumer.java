package com.xz.kafka.newApi.sasl.consumer;

import com.xz.kafka.conf.KafkaConf;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NewConsumer {

	/*
	java -cp :/usr/lib/kafka/libs/*:/xz/test/kafka.jar
	-Djava.security.auth.login.config=/etc/kafka/conf/kafka_client_jaas.conf
	-Djava.security.krb5.conf=/etc/krb5.conf
	com.xz.kafka.newApi.sasl.consumer.NewConsumer
	*/

	private static KafkaConsumer<String, String> consumer;

	public static Properties getConfig() {
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaConf.getBootstrapServers());
		props.put("group.id", "test2");

		props.put("enable.auto.commit", KafkaConf.getKafkaCommit());
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		//sasl
		props.put("security.protocol","SASL_PLAINTEXT");
		props.put("sasl.mechanism","GSSAPI");
		props.put("sasl.kerberos.service.name","kafka");
		return props;
	}

	public static void main(String[] args) {
		consumer = new KafkaConsumer<>(NewConsumer.getConfig());
		String topic = KafkaConf.getTopic() ;
		consumer.subscribe(Arrays.asList(topic));
		List<PartitionInfo> partitionInfos = consumer.listTopics().get(topic);
		System.out.println("分区数量"+partitionInfos.size());
		// 初始化 分区数量的线程池
		ExecutorService executorService = Executors.newFixedThreadPool(partitionInfos.size());
		for (int i = 0; i < partitionInfos.size(); i++) {

			NewConsumerTask runner = new NewConsumerTask(i, NewConsumer.getConfig(),topic);
			executorService.submit(runner);

		}
	}
}
