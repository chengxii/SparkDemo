package com.cx.core

import org.apache.spark.sql.SparkSession
import scala.math.random

/** 计算圆周率的近似值 */
object SparkPi {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkPi")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 2;
    val n = math.min(10000000L * slices, Int.MaxValue).toInt // avoid oerflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map{i =>
      val x = random * 2 -1
      val y = random * 2 -1
      if (x*x + y*y <= 1) 1 else 0
    }.reduce(_ + _)
    println(s"Pi is roughly ${4.0 * count / (n - 1)}")
    spark.stop()
  }
}
