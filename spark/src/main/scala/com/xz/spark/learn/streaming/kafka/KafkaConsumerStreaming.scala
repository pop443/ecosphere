package com.xz.spark.learn.streaming.kafka

import com.xz.spark.learn.config.PropertiesUtils
import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by Administrator on 2017-8-8.
  */
object KafkaConsumerStreaming extends Logging{
  val name = "KafkaConsumerStreaming"
  def main(args: Array[String]) {

    // 所有的streaming程序 如果使用本地跑的方式  必须使用local[n]
    // 如果使用local或者local[1] 则一个线程负责拉取数据 但是没有线程进行数据处理
    val sparkConf = new SparkConf().setAppName(name).setMaster("local[2]")
    //sparkConf.set("spark.com.xz.spark.learn.streaming.kafka.maxRatePerPartition", "5")
    // 2秒执行一次job executor 即 两秒的定时任务对拉取的数据进行操作
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("hdfs://xwtest1:8020/test/Checkpoint_Data")

    //kafka 配置信息
    val group = PropertiesUtils.getKafka_groupid()
    //设置需要读取的topic 和 topic的分区数量
    val topic = PropertiesUtils.getKafka_topic()
    val topicMap = Map(topic->3)
    val zkQuorum = PropertiesUtils.getZookeeper()
    //从kafka读取数据
    val input = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    val name1 = Thread.currentThread().getName
    println(name1+"======="+input.count().print())
    input.foreachRDD(rdd=>{
      val name2 = Thread.currentThread().getName
      val array = rdd.collect()
      array.foreach((tuple:Tuple2[String,String])=>{
        println(name2+"+++++++++++++++"+tuple._2)
      })
    })
    /*val lines = input.map(_._2)
    lines.foreachRDD((rdd:RDD[String])=>{
      val array = rdd.collect()
      array.foreach(x=> println("-----------"+x))
    })*/
    ssc.start()
    ssc.awaitTermination()
  }
}
