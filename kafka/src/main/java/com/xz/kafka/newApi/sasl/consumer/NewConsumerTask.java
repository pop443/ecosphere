package com.xz.kafka.newApi.sasl.consumer;

import com.xz.kafka.conf.KafkaConf;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class NewConsumerTask implements Runnable{
	private Integer partitionNum ;
	private Properties props ;
	private String topic ;
	public NewConsumerTask(int partitionNum, Properties props,String topic){
		this.partitionNum = partitionNum ;
		this.props = props ;
		this.topic = topic ;
	}
	
	@Override
	public void run() {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		TopicPartition partition0 = new TopicPartition(topic, partitionNum);
		consumer.assign(Arrays.asList(partition0));
		//method1(consumer);
		//method2(consumer);
		method3(consumer);
		
	}
	//测试用
	private void method3(KafkaConsumer<String, String> consumer) {
		while (true){
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.key()+"--"+record.value()+"--"+record.offset());
			}
			consumer.commitAsync();
		}
	}



}
