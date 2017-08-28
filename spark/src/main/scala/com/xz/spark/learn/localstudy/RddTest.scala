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
    //transationTest.transationFilter(sc)
    //transationTest.transationFlatMap(sc)
    //transationTest.transationDistinct1(sc)
    transationTest.transationDistinct2(sc)
    transationTest.transationDistinct3(sc)
  }
}

class TransationTest {
  //参数是函数，函数应用于RDD每一个元素，返回值是新的RDD
  def transationMap(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2 = rdd1.map(x => x > 2 && x < 5)
    val array1 = rdd2.collect()
    array1.foreach(x => println(x))
  }

  //参数是函数，函数会过滤掉不符合条件的元素，返回值是新的RDD
  def transationFilter(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    val rdd2 = rdd1.filter(x => x > 2 && x < 5)
    val array1 = rdd2.collect()
    array1.foreach(x => println(x))
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

  def transationDistinct1(sc: SparkContext): Unit = {
    val regex = Pattern.compile("_")
    val rdd1 = sc.makeRDD(Array("1", "1", "2", "3"), 2)
    val rdd2 = rdd1.distinct(2)
    rdd2.foreach(data => {
      println(data)
    })
  }

  def transationDistinct2(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(Array(new User1("x1", "15"), new User1("x2", "16"), new User1("x1", "16"), new User1("x2", "15"), new User1("x1", "15")), 2)
    val rdd2 = rdd1.distinct(2)
    rdd2.foreach(data => {
      println(data)
    })
  }

  def transationDistinct3(sc: SparkContext): Unit = {
    val rdd1 = sc.makeRDD(Array(new User2("x1", "15"), new User2("x2", "16"), new User2("x1", "16"), new User2("x2", "15"), new User2("x1", "15")), 2)
    val rdd2 = rdd1.distinct(2)
    rdd2.foreach(data => {
      println(data)
    })
  }


}

case class User1(name: String, age: String)

class User2 (name: String, age: String)
