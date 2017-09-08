package com.xz.kafka.newApi.plain_text.consumer;

/**
 * Created by Administrator on 2017-9-8.
 */
public class Main {
    public static void main(String[] args) {
        final NewConsumer newConsumer = new NewConsumer();
        newConsumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread("kafka-shutdown-hook"){
            @Override
            public void run() {
                newConsumer.stop();
            }
        });
    }
}
