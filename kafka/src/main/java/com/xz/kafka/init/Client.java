package com.xz.kafka.init;

import com.xz.kafka.conf.KafkaConf;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

public class Client {


	private AdminClient adminClient;
	private String topicName="test";

	@Before
	public void before(){
		Properties properties = new Properties();
		properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, KafkaConf.getBootstrapServers());
		adminClient = AdminClient.create(properties);
	}
	@Test
	public void createTopic(){
		NewTopic newTopic = new NewTopic(topicName,4, (short) 1);
		List<NewTopic> newTopicList = new ArrayList<>();
		newTopicList.add(newTopic);
		CreateTopicsResult createTopicsResult = adminClient.createTopics(newTopicList);
		System.out.println(createTopicsResult.all());
	}
	@Test
	public void listTopic(){
		ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
		listTopicsOptions.listInternal(true);
		ListTopicsResult result = adminClient.listTopics(listTopicsOptions);
		try {
			Collection<TopicListing> list = result.listings().get();
			list.forEach(topicListing -> {
				System.out.println(topicListing.name());
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void deleteTopic(){
		List<String> deleteList = new ArrayList<>();
		deleteList.add(topicName) ;
		DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(deleteList);
		System.out.println(deleteTopicsResult.all());
		/*try {
			for(Map.Entry<String,KafkaFuture<Void>> e : deleteTopicsResult.values().entrySet()){
                String topic=e.getKey();
                KafkaFuture<Void> future=e.getValue();
                Void v = future.get();
				System.out.println(topic+"---"+v.toString());
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}*/
	}
	
	@After
	public void after(){
		adminClient.close();
	}
}
