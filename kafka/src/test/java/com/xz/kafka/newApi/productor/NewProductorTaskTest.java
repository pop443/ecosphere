package com.xz.kafka.newApi.productor;

import com.xz.kafka.conf.KafkaConf;
import org.apache.kafka.clients.producer.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

public class NewProductorTaskTest implements Runnable {
	private Properties properties;
	private int num;
	private long time = 946656000000l ;
	private Random rp1 = new Random() ;
	private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss") ;

	public NewProductorTaskTest(Properties properties, int num) {
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
			String value = i+"" ;
			System.out.println(value);
			producer.send(new ProducerRecord<>("testreset2", key,
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
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			i++;
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
