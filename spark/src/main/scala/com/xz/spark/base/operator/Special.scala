package com.xz.spark.base.operator

/**
  * Created by Administrator on 2017-9-13.
  */
object Special {
  def main(args: Array[String]) {
    val special = new Special
    // ::
    special.doubleColon()
    // +:
    special.plusColon()
    // :+
    special.colonPlus()
    // ++
    special.plusPlus()
    // :::
    special.colonColonColon()
  }

}
class Special{
  /**
    * :: 该方法被称为cons，意为构造，向队列的头部追加数据，创造新的列表。
    * 用法为 x::list,其中x为加入到头部的元素，无论x是列表与否，它都只将成为新生成列表的第一个元素，
    * 也就是说新生成的列表长度为list的长度＋1(btw, x::list等价于list.::(x))
    */
  def doubleColon(): Unit ={
    val list = "A"::"B"::Nil
    println(list)
  }

  /**
    * +:方法用于在头部追加元素，和::很类似
    */
  def plusColon(): Unit ={
    val list = "A"+:"B"+:Nil
    println(list)
  }

  /**
    * :+方法用于在尾部追加元素
    */
  def colonPlus(): Unit ={
    val list = "A":+"B":+Nil
    println(list)
  }

  /**
    *
    */
  def plusPlus(): Unit ={
    val list = "A"++"B"++List(1)
    println(list)
  }

  /**
    *
    */
  def colonColonColon(): Unit ={
    val list = List("A")::: List("B")
    println(list)
  }
}