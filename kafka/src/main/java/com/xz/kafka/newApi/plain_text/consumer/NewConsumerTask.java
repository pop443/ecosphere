package com.xz.kafka.newApi.plain_text.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.xz.kafka.conf.KafkaConf;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.xz.hbase.conf.HbaseConf;
import com.xz.hbase.conf.HbaseConnection;
import com.xz.hbase.demo1.put.ConcurrentInsertTable;
import com.xz.hbase.demo1.put.InsertHbaseTable;
import com.xz.hbase.exception.XZException;
import com.xz.hbase.util.TableOperation;

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

	/**
	 * 一个链接所有数据
	 * @param consumer
	 */
	private void method2(KafkaConsumer<String, String> consumer) {
		Connection connection = HbaseConnection.getHbaseConnection() ;
		ConcurrentInsertTable concurrentInsertTable = null ;
		try {
			concurrentInsertTable = new ConcurrentInsertTable(TableName.valueOf(HbaseConf.TABLENAME), connection) ;
			List<String> valueList = new ArrayList<>() ;
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				for (ConsumerRecord<String, String> record : records) {
					valueList.add(record.value());
				}
				
				boolean bo = concurrentInsertTable.insert(valueList) ;
				if (KafkaConf.getKafkaCommit().equals("false") && bo) {
					consumer.commitAsync();
					System.out.println(valueList.size());
				}
				valueList.clear();
			}
			
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (XZException e) {
			e.printStackTrace();
		}finally{
			concurrentInsertTable.release();
		}
	}
	
	/**
	 * 一个链接一批数据
	 * @param consumer
	 */
	private void method1(KafkaConsumer<String, String> consumer) {
		List<String> valueList = new ArrayList<>() ;
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(1000);
			for (ConsumerRecord<String, String> record : records) {
				valueList.add(record.value());
			}
			//插入hbase
			TableOperation operation = new InsertHbaseTable(valueList) ;
			boolean bo = operation.handle(TableName.valueOf(HbaseConf.TABLENAME)) ;
			if (KafkaConf.getKafkaCommit().equals("false") && bo) {
				consumer.commitAsync();
				System.out.println(valueList.size());
			}
			valueList.clear();
		}
	}

}
