package com.cx.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ListeningNC {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    // 创建一个将要链接到hostname:port的DStream, 如 localhost:9999
    val lines = ssc.socketTextStream("localhost", 9999)
    // 将每一行拆分成 words（单词）
    val words = lines.flatMap(_.split(" "))

  }
}
