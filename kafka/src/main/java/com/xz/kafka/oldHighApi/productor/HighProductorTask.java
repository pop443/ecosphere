package com.xz.kafka.oldHighApi.productor;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class HighProductorTask implements Runnable {
	private ProducerConfig producerConfig;
	private int num;
	private long time = 946656000000l ;
	private String topic ;
	private Random rp1 = new Random() ;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss") ;

	public HighProductorTask(ProducerConfig producerConfig,String topic, int num) {
		this.producerConfig = producerConfig;
		this.topic = topic;
		this.num = num;
	}

	@Override
	public void run() {
		Producer<String, String> producer = new Producer<String, String>(producerConfig);
		int i = 1;
		while (i > 0) {
			String rowkey = this.getRowkey() ;
			String key = rowkey ;
			String value = rowkey+","+this.getcf()+","+this.getcf() ;
			System.out.println(value);
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic,i+"", value);
			producer.send(data);
			i++ ;
		}
	}
	
	private String getRowkey() {
		int p1 = rp1.nextInt(9999)+10000 ;
		String p2 = "_" ;
		long p3_long = Math.abs(rp1.nextLong() % 631152000000l) ;
		String p3 = dateFormat.format(new Date(time+p3_long)) ;
		String key = p1+p2+p3 ;
		return key;
	}
	
	private int getcf(){
		return rp1.nextInt(999)+1000 ;
	}

}
