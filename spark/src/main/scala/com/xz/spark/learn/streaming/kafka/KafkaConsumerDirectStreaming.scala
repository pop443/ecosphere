package com.xz.spark.learn.streaming.kafka

import com.xz.spark.learn.config.PropertiesUtils
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{Logging, SparkConf}

/**
  * Created by Administrator on 2017-8-8.
  */
object KafkaConsumerDirectStreaming extends Logging{
  val name = "KafkaConsumerDirectStreaming"
  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName(name).setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("hdfs://xwtest1:8020/test/Checkpoint_Dirct_Data")

    //kafka 配置信息
    val topic = PropertiesUtils.getKafka_topic()
    val topicsSet = Set(topic)
    val brokers = PropertiesUtils.getKafka_bootstrapservers()
    //从kafka读取数据
    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers
    )
    val input = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)
    val lines = input.map(_._2)
    val name1 = Thread.currentThread().getName
    println(name1)
    lines.foreachRDD(rdd=>{
      val name2 = Thread.currentThread().getName
      val array = rdd.collect()
      for (data <- array) {
        println(name2+"----------"+data)
      }
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
