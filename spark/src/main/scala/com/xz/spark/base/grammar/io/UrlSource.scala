package com.xz.spark.base.grammar.io

import scala.io.Source

/**
  * Created by Administrator on 2017-8-30.
  */
object UrlSource {
  def main(args: Array[String]) {
    val webFile = Source.fromURL("http://www.baidu.com")
    for (x <- webFile.getLines()) {
      println(x)
    }
    webFile.close()
  }
}
