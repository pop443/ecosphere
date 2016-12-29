package com.xz.kafka.oldHighApi.consumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class HighConsumerTask implements Runnable{
	private KafkaStream<byte[], byte[]> m_stream;
	private int num;

	public HighConsumerTask(KafkaStream<byte[], byte[]> m_stream, int num) {
		this.m_stream = m_stream;
		this.num = num;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> iterator = m_stream.iterator();
		System.out.println("m_threadNumber:" + num);
		while (iterator.hasNext()) {
			String msg = new String(iterator.next().message());
			System.out.println(msg);
		}
	}


}
