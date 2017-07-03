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

	/*/usr/java/jdk1.8.0_101/bin/java -cp :
	/usr/lib/ambari-metrics-kafka-sink/ambari-metrics-kafka-sink.jar:
	/usr/lib/ambari-metrics-kafka-sink/lib*//*:
	/usr/lib/kafka/bin/../libs/aopalliance-repackaged-2.4.0-b34.jar:
	/usr/lib/kafka/bin/../libs/argparse4j-0.5.0.jar:
	/usr/lib/kafka/bin/../libs/connect-api-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/connect-file-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/connect-json-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/connect-runtime-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/guava-18.0.jar:
	/usr/lib/kafka/bin/../libs/hk2-api-2.4.0-b34.jar:
	/usr/lib/kafka/bin/../libs/hk2-locator-2.4.0-b34.jar:
	/usr/lib/kafka/bin/../libs/hk2-utils-2.4.0-b34.jar:
	/usr/lib/kafka/bin/../libs/jackson-annotations-2.6.0.jar:
	/usr/lib/kafka/bin/../libs/jackson-core-2.6.3.jar:
	/usr/lib/kafka/bin/../libs/jackson-databind-2.6.3.jar:
	/usr/lib/kafka/bin/../libs/jackson-jaxrs-base-2.6.3.jar:
	/usr/lib/kafka/bin/../libs/jackson-jaxrs-json-provider-2.6.3.jar:
	/usr/lib/kafka/bin/../libs/jackson-module-jaxb-annotations-2.6.3.jar:
	/usr/lib/kafka/bin/../libs/javassist-3.18.2-GA.jar:
	/usr/lib/kafka/bin/../libs/javax.annotation-api-1.2.jar:
	/usr/lib/kafka/bin/../libs/javax.inject-1.jar:
	/usr/lib/kafka/bin/../libs/javax.inject-2.4.0-b34.jar:
	/usr/lib/kafka/bin/../libs/javax.servlet-api-3.1.0.jar:
	/usr/lib/kafka/bin/../libs/javax.ws.rs-api-2.0.1.jar:
	/usr/lib/kafka/bin/../libs/jersey-client-2.22.2.jar:
	/usr/lib/kafka/bin/../libs/jersey-common-2.22.2.jar:
	/usr/lib/kafka/bin/../libs/jersey-container-servlet-2.22.2.jar:
	/usr/lib/kafka/bin/../libs/jersey-container-servlet-core-2.22.2.jar:
	/usr/lib/kafka/bin/../libs/jersey-guava-2.22.2.jar:
	/usr/lib/kafka/bin/../libs/jersey-media-jaxb-2.22.2.jar:
	/usr/lib/kafka/bin/../libs/jersey-server-2.22.2.jar:
	/usr/lib/kafka/bin/../libs/jetty-continuation-9.2.15.v20160210.jar:
	/usr/lib/kafka/bin/../libs/jetty-http-9.2.15.v20160210.jar:
	/usr/lib/kafka/bin/../libs/jetty-io-9.2.15.v20160210.jar:
	/usr/lib/kafka/bin/../libs/jetty-security-9.2.15.v20160210.jar:
	/usr/lib/kafka/bin/../libs/jetty-server-9.2.15.v20160210.jar:
	/usr/lib/kafka/bin/../libs/jetty-servlet-9.2.15.v20160210.jar:
	/usr/lib/kafka/bin/../libs/jetty-servlets-9.2.15.v20160210.jar:
	/usr/lib/kafka/bin/../libs/jetty-util-9.2.15.v20160210.jar:
	/usr/lib/kafka/bin/../libs/jopt-simple-4.9.jar:
	/usr/lib/kafka/bin/../libs/kafka_2.11-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/kafka_2.11-0.10.1.1-sources.jar:
	/usr/lib/kafka/bin/../libs/kafka_2.11-0.10.1.1-test-sources.jar:
	/usr/lib/kafka/bin/../libs/kafka-clients-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/kafka-log4j-appender-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/kafka-streams-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/kafka-streams-examples-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/kafka-tools-0.10.1.1.jar:
	/usr/lib/kafka/bin/../libs/log4j-1.2.17.jar:
	/usr/lib/kafka/bin/../libs/lz4-1.3.0.jar:
	/usr/lib/kafka/bin/../libs/metrics-core-2.2.0.jar:
	/usr/lib/kafka/bin/../libs/osgi-resource-locator-1.0.1.jar:
	/usr/lib/kafka/bin/../libs/reflections-0.9.10.jar:
	/usr/lib/kafka/bin/../libs/rocksdbjni-4.9.0.jar:
	/usr/lib/kafka/bin/../libs/scala-library-2.11.8.jar:
	/usr/lib/kafka/bin/../libs/scala-parser-combinators_2.11-1.0.4.jar:
	/usr/lib/kafka/bin/../libs/slf4j-api-1.7.21.jar:
	/usr/lib/kafka/bin/../libs/slf4j-log4j12-1.7.21.jar:
	/usr/lib/kafka/bin/../libs/snappy-java-1.1.2.6.jar:
	/usr/lib/kafka/bin/../libs/validation-api-1.1.0.Final.jar:
	/usr/lib/kafka/bin/../libs/zkclient-0.9.jar:
	/usr/lib/kafka/bin/../libs/zookeeper-3.4.8.jar:
	/xz/test/kafka.jar
	-Djava.security.auth.login.config=/etc/kafka/conf/kafka_client_jaas.conf
	-Djava.security.krb5.conf=/etc/krb5.conf
	com.xz.kafka.newApi.sasl.consumer.NewConsumer*/

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
