package com.xz.spark.learn.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017-8-8.
  */
object SocketStreaming {
  val name = "socket_test"
  def main(args: Array[String]) {


    val sparkConf = new SparkConf().setAppName(name).setMaster("local")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val lines = ssc.socketTextStream("localhost",8080,StorageLevel.MEMORY_AND_DISK)
    lines.foreachRDD((rdd:RDD[String])=>{
      rdd.map(x=>println(x))
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
