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
import org.apache.log4j.Logger;

public class NewConsumerTask implements Runnable {
    private Logger log = Logger.getLogger(NewConsumer.class);
    private Integer partitionNum;
    private Properties props;
    private String topic;
    private boolean isRun;

    public NewConsumerTask(int partitionNum, Properties props, String topic) {
        this.partitionNum = partitionNum;
        this.props = props;
        this.topic = topic;
        this.isRun = false;
    }

    @Override
    public void run() {
        this.isRun = true ;
        log.info(Thread.currentThread().getName()+"--- start");
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = new KafkaConsumer<>(props);
            TopicPartition partition0 = new TopicPartition(topic, partitionNum);
            consumer.assign(Arrays.asList(partition0));
            //method1(consumer);
            //method2(consumer);
            method3(consumer);
        } catch (Exception e) {
            e.printStackTrace();
            consumer.close();
            log.error(Thread.currentThread().getName()+"--- error");
            this.isRun = false ;
        }
    }

    public boolean isRun() {
        return isRun;
    }

    //测试用
    private void method3(KafkaConsumer<String, String> consumer) throws Exception {
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(20000);
            for (ConsumerRecord<String, String> record : records) {
                String key = record.key();
                String value = record.value();
                long offset = record.offset();
                if (value.equals("8")){
                    throw new Exception("error");
                }
                System.out.println(key + "--" + value + "--" + offset);
            }
            //consumer.commitAsync();
        }
    }

    /**
     * 一个链接所有数据
     *
     * @param consumer
     */
    private void method2(KafkaConsumer<String, String> consumer) {
        Connection connection = HbaseConnection.getHbaseConnection();
        ConcurrentInsertTable concurrentInsertTable = null;
        try {
            concurrentInsertTable = new ConcurrentInsertTable(TableName.valueOf(HbaseConf.TABLENAME), connection);
            List<String> valueList = new ArrayList<>();
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);
                for (ConsumerRecord<String, String> record : records) {
                    valueList.add(record.value());
                }

                boolean bo = concurrentInsertTable.insert(valueList);
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
        } finally {
            concurrentInsertTable.release();
        }
    }

    /**
     * 一个链接一批数据
     *
     * @param consumer
     */
    private void method1(KafkaConsumer<String, String> consumer) {
        List<String> valueList = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                valueList.add(record.value());
            }
            //插入hbase
            TableOperation operation = new InsertHbaseTable(valueList);
            boolean bo = operation.handle(TableName.valueOf(HbaseConf.TABLENAME));
            if (KafkaConf.getKafkaCommit().equals("false") && bo) {
                consumer.commitAsync();
                System.out.println(valueList.size());
            }
            valueList.clear();
        }
    }

}
