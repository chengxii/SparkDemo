package com.cx.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 延迟实例化单例SparkSession实例
  */
object SparkSessionSingleton {

  @transient private[this] var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("cx-demo")
    .setMaster("local[2]")
}
