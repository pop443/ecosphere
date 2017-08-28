package com.xz.spark.learn.config

import java.io.InputStream
import java.util.Properties

/**
  * Created by Administrator on 2017-8-10.
  */
object PropertiesUtils {
  private val properties = new PropertiesUtils("config.properties")
  def getConfig(): Properties ={
    properties.getConfig()
  }

  def getKafka_topic(): String ={
    getConfig().getProperty("topic")
  }

  def getKafka_bootstrapservers(): String ={
    getConfig().getProperty("bootstrapservers")
  }
  def getKafka_groupid(): String ={
    getConfig().getProperty("groupid")
  }
  def getZookeeper(): String ={
    getConfig().getProperty("zookeeper")
  }
  def getHbase_znode(): String ={
    getConfig().getProperty("hbase_znode")
  }

  def main(args: Array[String]) {
    val a = PropertiesUtils.getKafka_topic()
    println(a)
  }
}

class PropertiesUtils private(string:String){
  def getConfig(): Properties ={
    var input:InputStream = null
    try {
      input = this.getClass.getClassLoader.getResourceAsStream(string)
      val config = new Properties()
      config.load(input)
      input.close()
      config
    } catch {
      case ex: Exception => print("==== init properties failed " + ex.toString)
        null
    } finally {
      if (input != null) {
        input.close()
      }
    }
  }
}