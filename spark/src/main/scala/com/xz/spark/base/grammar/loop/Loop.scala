package com.xz.spark.base.grammar.loop

/**
  * Created by Administrator on 2017-7-22.
  */
object ArrayLoop {
  def main(args: Array[String]) {
    val array:Array[Int] = Array(1,2,3)
    val loop = new ArrayLoop(array)
    loop.l_map()
    //loop.l_foreach()
    //loop.l_for_to()
    //loop.l_for_until()
    //loop.l_for_range()
  }

}

class ArrayLoop(array:Array[Int]){
  def l_foreach(): Unit ={
    println("--- l_foreach --")
    for(a <-array){
      println(a)
    }
  }
  def l_for_to(): Unit ={
    println("--- l_for_to --")
    for(i<-0 to array.length-1){
      println(array(i))
    }
  }
  def l_for_until(): Unit ={
    println("--- l_for_until --")
    for(i<-0 until array.length){
      println(array(i))
    }
  }
  def l_for_range(): Unit ={
    println("--- l_for_range --")
    for(i<-array.indices){
      println(array(i))
    }
  }
  def l_map(): Unit ={
    println("--- l_map --")
    val v2 = array.map((_ ,1 ))
    println(v2.getClass)
    for(i<-v2.indices){
      println(v2(i))
    }
  }

}