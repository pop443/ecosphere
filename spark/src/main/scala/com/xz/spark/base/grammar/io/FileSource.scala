package com.xz.spark.base.grammar.io

import java.io.PrintWriter

import scala.io.Source
import java.io.File

/**
  * Created by Administrator on 2017-8-21.
  */
object FileSource {
  def main(args: Array[String]) {
    val file = Source.fromFile("C:\\Users\\Administrator\\Desktop\\1\\spark_137.log")
    val outFile = new PrintWriter(new File("C:\\Users\\Administrator\\Desktop\\1\\error.log"))
    for (x<-file.getLines()){
      if (x.contains("Exception")){
        outFile.write(x)
      }
    }
    file.close()
    outFile.close()
  }
}
