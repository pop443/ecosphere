package com.xz.kafka.newApi.productor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

import com.xz.kafka.conf.KafkaConf;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class NewProductorTask implements Runnable {
	private Properties properties;
	private int num;
	private long time = 946656000000l ;
	private Random rp1 = new Random() ;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss") ;

	public NewProductorTask(Properties properties, int num) {
		this.properties = properties;
		this.num = num;
	}

	@Override
	public void run() {
		Producer<String, String> producer = new KafkaProducer<>(properties);
		int i = 1;
		while (i > 0) {
			//key = value = 10000_20000101000000  19999_20200101000000
			String rowkey = this.getRowkey() ;
			String key = rowkey ;
			String value = rowkey+","+this.getcf()+","+this.getcf() ;
//			System.out.println(value);
			producer.send(new ProducerRecord<>(KafkaConf.getTopic(), key,
					value), new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
						e.printStackTrace();
					}
					/*System.out.println("Thread num:" + num
							+ "---partition:"+metadata.partition()+",offset: "
							+ metadata.offset());*/
				}
			});
			producer.flush();
		}
		producer.close();
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
