package com.xz.spark.learn.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017-8-8.
  */
object FileStreaming {
  val name = "kafka_test"
  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName(name).setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("E:\\test\\spark")

    //kafka 配置信息
    val group = "group_test"
    val topics = "topic_test"
    val numThreads = "1"
    val zkQuorum = "xwtest1,xwtest2,xwtest3"
    //从kafka读取数据
    val topicMap = Map(topics -> 2)
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    lines.foreachRDD((rdd:RDD[String])=>{
      val array = rdd.collect()
      array.foreach(x=> println("------------"+x))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
