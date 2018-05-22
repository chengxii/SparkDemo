package com.cx.sql

import com.cx.utils.SparkWrapper
import org.apache.spark.sql.SparkSession

/**
  * @author xi.cheng
  */
object SQLDataSourceExample {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkWrapper(appName = "Spark SQL data sources example", master = "local[*]").getSparkSession

  }
}
