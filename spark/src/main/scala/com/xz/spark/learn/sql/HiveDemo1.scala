package com.xz.spark.learn.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-8-10.
  */
object HiveDemo1 {
  val name = "HiveDemo1"
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(name).setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    // hive版本 默认1.2.1
    hiveContext.setConf("spark.com.xz.spark.learn.sql.hive.metastore.version","1.2.1")
    // spark 是否使用自己的序列化和反序列化方式 默认为true
    hiveContext.setConf("spark.sql.hive.convertMetastoreParquet","true")

    hiveContext.sql("CREATE TABLE IF NOT EXISTS xz_test (key int, value string)")

    hiveContext.sql("select  a.userlable,a.usergroup\n  from (select distinct userlable, usergroup\n          from cloginserverlog\n         where ptdate = '20170815'\n           and substring(servertime, 0, 10) = '2017081517') a\n  left join (select u.userlable from uservisitrecordhis_offline u \n  left join uservisitrecordhis_offnetwork h\n  on u.userlable=h.userlable where h.userlable is null) b\n    on a.userlable = b.userlable\n where b.userlable is null").collect()
  }
}
