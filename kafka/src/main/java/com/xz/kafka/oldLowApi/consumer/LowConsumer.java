package com.xz.kafka.oldLowApi.consumer;

import com.xz.kafka.conf.KafkaConf;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.BrokerEndPoint;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * falcon -- 2016/12/29.
 */
public class LowConsumer {


    private List<String> m_replicaBrokers = null ;

    public LowConsumer(){
        //副本存在的borker
        m_replicaBrokers = new ArrayList<>() ;
    }

    public void run(long a_maxReads,String a_topic,int a_partition,List<String> a_seedBrokers,int a_port) throws Exception{
        //分区元数据信息
        PartitionMetadata metadata = findLeader(a_seedBrokers,a_port,a_topic,a_partition) ;
        if (metadata==null) {
            System.err.println("can't find metadata for topic and partition.exiting");
            return ;
        }
        if (metadata.leader() == null) {
            System.err.println("can't find leader for topic and partition.exiting");
            return ;
        }

        String leadBroker = metadata.leader().host() ;
        String clientName = "client_"+a_topic+"_"+a_partition ;

        SimpleConsumer consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64*1024, clientName) ;
        //获得 这个topic 下的单个分区的偏移量  参数4 表示从什么时间开始
        long readoffset = getLastOffset(consumer,a_topic,a_partition,OffsetRequest.EarliestTime(),clientName) ;
        System.out.println("偏移量："+readoffset);
        int numErrors = 0 ;
//		while (a_maxReads>0) {
//			if (consumer==null) {
//				consumer = new SimpleConsumer(leadBroker, a_port, 100000, 64*1024, clientName) ;
//			}
        FetchRequest request = new FetchRequestBuilder().clientId(clientName).addFetch(a_topic, a_partition, readoffset, 100000).build() ;
        FetchResponse fetchResponse = consumer.fetch(request) ;

//			if (fetchResponse.hasError()) {
//				numErrors++ ;
//				short code = fetchResponse.errorCode(a_topic,a_partition) ;
//				System.out.println("Error fetching data from the brokers:"+leadBroker+"Reason:"+code);
//				if (numErrors>5) {
//					break ;
//				}
//				if (code == ErrorMapping.OffsetOutOfRangeCode()) {
//					readoffset = getLastOffset(consumer, a_topic, a_partition, OffsetRequest.LatestTime(), clientName) ;
//					continue ;
//				}
//				consumer.close();
//				consumer = null ;
//				leadBroker = findNewLeader(leadBroker,a_topic,a_partition,a_port) ;
//				continue ;
//			}
        numErrors = 0 ;
        long numRead = 0 ;
        ByteBufferMessageSet bufferMessageSet = fetchResponse.messageSet(a_topic, a_partition) ;
        Iterator<MessageAndOffset> it = bufferMessageSet.iterator() ;
        while (it.hasNext()) {
            MessageAndOffset messageAndOffset = it.next() ;
            long currentOffset = messageAndOffset.offset() ;
            ByteBuffer payload = messageAndOffset.message().payload() ;

            byte[] bytes = new byte[payload.limit()] ;
            payload.get(bytes) ;
            System.out.println(String.valueOf(messageAndOffset.offset())+":"+ Bytes.toString(bytes));
            numRead++ ;
//				a_maxReads-- ;
        }
        if (numRead==0) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//		}
        if (consumer!=null) {
            consumer.close();
        }

    }




    private String findNewLeader(String a_oldLeader, String a_topic, int a_partition, int a_port) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false ;
            PartitionMetadata metadata = findLeader(m_replicaBrokers, a_port, a_topic, a_partition) ;
            if (metadata == null) {
                goToSleep = true ;
            }else if (metadata.leader()==null) {
                goToSleep = true ;
            }else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i==0) {
                goToSleep = true ;
            }else {
                return metadata.leader().host() ;
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        System.out.println("Unable to find new leader after broker failure.exiting");
        throw new Exception("Unable to find new leader after broker failure.exiting") ;
    }

    private long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime,
                               String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition) ;
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>() ;
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1)) ;
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, OffsetRequest.CurrentVersion(), clientName) ;
        OffsetResponse response = consumer.getOffsetsBefore(request) ;
        if (response.hasError()) {
            System.out.println("Error fetching data offset data the broker . Reason: "+response.errorCode(topic, partition)) ;
            return 0 ;
        }
        long[] offsets = response.offsets(topic, partition) ;
        System.out.println("------------偏移量------------");
        for (int i = 0; i < offsets.length; i++) {
            System.out.println(offsets[i]);
        }
        System.out.println("------------偏移量------------");
        return offsets[0] ;
    }

    private PartitionMetadata findLeader(List<String> a_seedBrokers, int a_port, String a_topic, int a_partition) {
        PartitionMetadata returnMetadata = null ;
        loop:
        for (int i = 0; i < a_seedBrokers.size(); i++) {
            String seed = a_seedBrokers.get(i) ;
            SimpleConsumer consumer = null ;
            try {
                consumer = new SimpleConsumer(seed, a_port, 100000, 64*1024, "leaderLookup") ;
                List<String> topics = Collections.singletonList(a_topic) ;
                //从topic 获得请求数据结构
                TopicMetadataRequest request = new TopicMetadataRequest(topics) ;
                //获得 topic的响应 分区信息等
                TopicMetadataResponse resp = consumer.send(request) ;

                List<TopicMetadata> metadata = resp.topicsMetadata() ;
                for (int j = 0; j < metadata.size(); j++) {
                    TopicMetadata topicMetadata = metadata.get(j) ;
                    List<PartitionMetadata> partitionMetadatas = topicMetadata.partitionsMetadata() ;
                    System.out.println("一共"+partitionMetadatas.size()+"分区。");
                    for (int k = 0; k < partitionMetadatas.size(); k++) {
                        PartitionMetadata partitionMetadata = partitionMetadatas.get(k) ;
                        System.out.println("分区号："+partitionMetadata.partitionId());
                        if (partitionMetadata.partitionId()==a_partition) {
                            returnMetadata = partitionMetadata ;
                            break loop ;
                        }
                    }

                }
            } catch (Exception e) {
                System.out.println("error communicating with broker["+seed
                        +"]to find leader for ["+a_topic+" , "+a_partition+"] reasopn:"+e);
            }finally{
                if (consumer!=null) {
                    consumer.close();
                }
            }
        }
        if (returnMetadata!=null) {
            m_replicaBrokers.clear();
            List<BrokerEndPoint> list = returnMetadata.replicas() ;
            for (int i = 0; i < list.size(); i++) {
                BrokerEndPoint brokerEndPoint =  list.get(i) ;
                System.out.println(brokerEndPoint.connectionString());
                m_replicaBrokers.add(brokerEndPoint.host()) ;
            }
        }
        return returnMetadata;
    }

    public static void main(String[] args) {
        LowConsumer comsumer = new LowConsumer() ;
        long maxReads = 10 ;
        String topic = KafkaConf.getTopic();
        System.out.println("分区名为"+topic);
        int partition = 0 ;

        String broker = KafkaConf.getBootstrapServers() ;
        String[] brokers = broker.split(",") ;
        List<String> seeds = new ArrayList<>() ;
        for (int i = 0; i < brokers.length; i++) {
            seeds.add(brokers[i].split(":")[0]) ;
        }
        int port = 9092 ;

        try {
            comsumer.run(maxReads, topic, partition, seeds, port);
        } catch (Exception e) {
            System.out.println("Oops:"+e.getMessage());
            e.printStackTrace();
        }

    }

}
