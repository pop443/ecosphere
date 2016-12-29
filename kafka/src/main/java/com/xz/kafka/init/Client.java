package com.xz.kafka.init;

import com.xz.kafka.conf.KafkaConf;
import kafka.admin.TopicCommand;
import kafka.admin.TopicCommand.TopicCommandOptions;
import kafka.utils.ZkUtils;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

public class Client {
	private ZkUtils zkUtils = new ZkUtils(
			new ZkClient(KafkaConf.getZookeeper()), new ZkConnection(
					KafkaConf.getZookeeper()), true);

	private void listTopic(){
		String [] options = new String[]{
				"--zookeeper",
				KafkaConf.getZookeeper()
		};
		TopicCommandOptions topicCommandOptions = new TopicCommandOptions(options) ;
		TopicCommand.listTopics(zkUtils, topicCommandOptions);
	}
	private void createTopic(){
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
		TopicCommand.createTopic(zkUtils, topicCommandOptions);
	}
	
	private void deleteTopic(){
		String[] options = new String[]{
				"--zookeeper",
				KafkaConf.getZookeeper(),
				"--topic",
				KafkaConf.getTopic()
		};
		TopicCommandOptions topicCommandOptions = new TopicCommandOptions(options) ;
		TopicCommand.deleteTopic(zkUtils, topicCommandOptions);
	}
	
	public static void main(String[] args) {
		//错误 -- 未生成分区分文件夹
		Client client = new Client() ;
		client.deleteTopic();
		//client.createTopic();
		//client.listTopic();
	}
}
