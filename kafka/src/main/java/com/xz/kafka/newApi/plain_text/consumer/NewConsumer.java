package com.xz.kafka.newApi.plain_text.consumer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.xz.kafka.conf.KafkaConf;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.log4j.Logger;

public class NewConsumer {
	private ExecutorService executor;
	private ScheduledExecutorService listener;
	private Logger log = Logger.getLogger(NewConsumer.class);

	public static Properties getConfig() {
		Properties props = new Properties();
		props.put("bootstrap.servers", KafkaConf.getBootstrapServers());
		props.put("group.id", KafkaConf.getGroupId());
		//auto.offset.reset 1.earliest 2.latest 3.none
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put("enable.auto.commit", KafkaConf.getKafkaCommit());
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		//数据拉取很多 但是数据一次只处理2条
		props.put("max.poll.records", "2");
		props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

	public synchronized void start(){
		listener = Executors.newSingleThreadScheduledExecutor();
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(NewConsumer.getConfig());
		String topic = KafkaConf.getTopic() ;
		List<PartitionInfo> partitionInfos = consumer.listTopics().get(topic);
		log.info("分区数量"+partitionInfos.size());
		// 初始化 分区数量的线程池
		executor = Executors.newFixedThreadPool(partitionInfos.size());
		Linstener linstener = new Linstener(executor) ;
		for (int i = 0; i < partitionInfos.size(); i++) {
			NewConsumerTask runner = new NewConsumerTask(i, NewConsumer.getConfig(),topic);
			executor.submit(runner);
			linstener.add(runner);
		}
		consumer.close();
		listener.scheduleWithFixedDelay(linstener,0,20,TimeUnit.SECONDS);
	}

	public synchronized void stop(){
		this.executor.shutdown();
		this.listener.shutdown();
		try {
			this.executor.awaitTermination(10L, TimeUnit.SECONDS);
			this.listener.awaitTermination(10L, TimeUnit.SECONDS);
		} catch (InterruptedException var2) {
			log.info("Interrupted while awaiting termination", var2);
		}

		this.executor.shutdownNow();
		this.listener.shutdownNow();
	}
}
