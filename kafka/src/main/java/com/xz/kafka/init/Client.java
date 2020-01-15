package com.xz.kafka.init;

import com.xz.kafka.conf.KafkaConf;
import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.utils.ZkUtils;

import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.utils.SystemTime;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class Client {


	private KafkaZkClient kafkaZkClient ;

	@Before
	public void before(){
		ZooKeeperClient zooKeeperClient = new ZooKeeperClient(KafkaConf.getZookeeper(),30000,30000,30000,new SystemTime(),"","") ;
		kafkaZkClient = new KafkaZkClient(zooKeeperClient,false,new SystemTime());
	}
	@Test
	public void listTopic(){
		String [] options = new String[]{
				"--zookeeper",
				KafkaConf.getZookeeper()
		};
		TopicCommandOptions topicCommandOptions = new TopicCommandOptions(options) ;
		TopicCommand.listTopics(kafkaZkClient, topicCommandOptions);
	}
	@Test
	public void createTopic(){
		String[] options = new String[]{
				"--zookeeper",
				KafkaConf.getZookeeper(),
				"--topic",
				KafkaConf.getTopic(),
				"--partitions",
				"2",
				"--replication-factor",
				"1"
		};
		TopicCommandOptions topicCommandOptions = new TopicCommandOptions(options) ;
		TopicCommand.createTopic(kafkaZkClient, topicCommandOptions);
	}

	@Test
	public void deleteTopic(){
		String[] options = new String[]{
				"--zookeeper",
				KafkaConf.getZookeeper(),
				"--topic",
				KafkaConf.getTopic()
		};
		TopicCommandOptions topicCommandOptions = new TopicCommandOptions(options) ;
		TopicCommand.deleteTopic(kafkaZkClient, topicCommandOptions);
	}
	
	@After
	public void after(){
		kafkaZkClient.close();
	}
}
