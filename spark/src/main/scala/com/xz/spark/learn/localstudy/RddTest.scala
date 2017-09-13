package com.xz.spark.learn.localstudy

import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2017-8-17.
  */
object RddTest {
  def main(args: Array[String]) {
    val transationTest = new TransationTest
    val sparkconf = new SparkConf().setMaster("local").setAppName("rddTest")
    val sc = new SparkContext(sparkconf)

    //transationTest.transationMap(sc)
    //transationTest.transationFlatMap(sc)
    //transationTest.transationMapPartitions(sc)
    //transationTest.transationGlom(sc)

    //transationTest.transationUnion1(sc)
    //transationTest.transationUnion2(sc)

    //transationTest.transationFilter(sc)
    //transationTest.transationDistinct1(sc)
    //transationTest.transationDistinct2(sc)
    //transationTest.transationDistinct3(sc)

  }
}

class TransationTest {

  //参数是函数，函数应用于RDD每一个元素 返回迭代器，返回值是新的RDD
  def transationMap(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2 = rdd1.map(x => x > 2 && x < 5)
    rdd2.foreach(x => println(x))
  }

  //参数是函数，函数应用于RDD每一个元素，将元素数据进行拆分，变成迭代器，返回值是新的RDD
  //rdd "1_2","3_4","5-6" => rdd "1","2","3","4","5-6"
  def transationFlatMap(sc: SparkContext): Unit = {
    val regex = Pattern.compile("_")
    val rdd1 = sc.makeRDD(List("1_2", "3_4", "5-6"), 2)
    val rdd2 = rdd1.flatMap(x => regex.split(x))
    rdd2.foreach(data => {
      println(data)
    })
  }

  //参数是函数，函数应用于每一组
  def transationMapPartitions(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2 = rdd1.mapPartitions(x => {
      var i = 0
      while (x.hasNext) {
        i += x.next()
      }
      List(i).iterator
    })
    val array1 = rdd2.collect()
    array1.foreach(x => println(x))
  }

  //将rdd重每个分区的数据，合并成一个数组 返回rdd[Array]
  def transationGlom(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2 = rdd1.glom()
    rdd2.foreach((x:Array[Int])=>{
      x.foreach((y:Int) =>println(y))
    })
  }

  //将rdd重每个分区的数据，合并成一个数组 返回rdd[Array]
  def transationUnion1(sc: SparkContext): Unit = {
    val rdd1=sc.parallelize(List(('a',2),('b',4),('c',6),('d',9)),2)
    val rdd2=sc.parallelize(List(('c',6),('c',7),('d',8),('e',10)),2)
    val rdd3=rdd1 union rdd2
    rdd3.foreach((x:(Char,Int))=>{
      println(x._1+"---"+x._2)
    })

  }

  def transationUnion2(sc: SparkContext): Unit = {
    val rdd1=sc.parallelize(List(1,2,3,4),2)
    val rdd2=sc.parallelize(List(4,5,6,8),2)
    val rdd3=rdd1 union rdd2
    val rdd4 = rdd3.mapPartitions(x=>{
      val result = List[Int]()
      var i = 0
      while (x.hasNext) {
        i += x.next()
      }
      result.::(i).iterator
    })
    val array1 = rdd4.collect()
    var i = 0 ;
    array1.foreach(x => i=i+x)
    println(i)
  }

  //参数是函数，函数会过滤掉不符合条件的元素，返回值是新的RDD
  def transationFilter(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2 = rdd1.filter(x => x > 2 && x < 5)
    val array1 = rdd2.collect()
    array1.foreach( (x:Int) => println(x))
  }


  //简单类型去重
  def transationDistinct1(sc: SparkContext): Unit = {
    val regex = Pattern.compile("_")
    val rdd1 = sc.makeRDD(Array("1", "1", "2", "3"), 2)
    val rdd2 = rdd1.distinct(2)
    rdd2.foreach(data => {
      println(data)
    })
  }

  //case对象 去重
  def transationDistinct2(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(Array(new User1("x1", "15"), new User1("x2", "16"), new User1("x1", "16"), new User1("x2", "15"), new User1("x1", "15")))
    val rdd2 = rdd1.distinct(2)
    rdd2.foreach(data => {
      println(data)
    })
  }

  // 对象（需要序列化 重写equal和hashcode方法） 去重
  def transationDistinct3(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(Array(new User2("x1", "15"), new User2("x2", "16"), new User2("x1", "16"), new User2("x2", "15"), new User2("x1", "15")), 2)
    val rdd2 = rdd1.distinct(2)
    rdd2.foreach(data => {
      println(data)
    })
  }


}

case class User1(name: String, age: String)

class User2(val name1: String, val age1: String) extends Serializable {
  var name = name1
  var age = age1

  override def toString: String = {
    "User2(" + name + "," + age + ")"
  }

  def canEqual(other: Any): Boolean = other.isInstanceOf[User2]

  override def equals(other: Any): Boolean = other match {
    case that: User2 =>
      (that canEqual this) &&
        name1 == that.name1 &&
        age1 == that.age1
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(name1, age1)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}
