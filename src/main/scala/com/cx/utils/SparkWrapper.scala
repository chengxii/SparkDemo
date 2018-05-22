package com.cx.utils

import java.util.Properties
import org.apache.spark.sql.{DataFrame, DataFrameReader, SQLContext, SparkSession}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map

/**
  * @author xi.cheng
  */
class SparkWrapper(appName: String = "sparkLocal",
                   master: String = null,
                   setting: Map[String, String] = Map(),
                   streamingMilliSecondsDuration: Long = 0) {
  private var sparkConf: SparkConf = null
  private var sparkSession: SparkSession = null
  private var sc: SparkContext = null
  private var sqlContext: SQLContext = null
  private var streamingContext: StreamingContext = null

  def init(): this.type = {
    this.sparkConf = new SparkConf().setAppName(appName)
    if (master != null) this.sparkConf.setMaster(master)
    if (setting != null) setting.foreach(e => {
      val (k, v) = e; this.sparkConf.set(k, v)
    })
    if (streamingMilliSecondsDuration > 0) {
      this.streamingContext = new StreamingContext(this.sparkConf, Milliseconds(streamingMilliSecondsDuration))
    } else {
      this.sparkSession = SparkSession.builder.appName(appName).master(master).config(this.sparkConf).getOrCreate()
      this.sc = this.sparkSession.sparkContext
      this.sqlContext = this.sparkSession.sqlContext
    }
    this
  }

  def newSparkSessionFromConf(sparkConf: SparkConf): SparkSession = {
    return SparkSession.builder().config(sparkConf).getOrCreate()
  }

  def getSparkContext(): SparkContext = {
    this.sc
  }

  def getSparkSession(): SparkSession = {
    this.sparkSession
  }

  def getSQLContext(): SQLContext = {
    this.sqlContext
  }

  def getStreamingContext(): StreamingContext = {
    this.streamingContext
  }

  def stop() = {
    this.sqlContext = null
    if (this.sc != null) this.sc.stop()
    if (this.sparkSession != null) this.sparkSession.stop()
    if (this.streamingContext != null) this.streamingContext.stop()
  }
}

object SparkWrapper {
  private var remoteSetting = Map[String, String]()

  /**
    * 调用例子:
    * import com.stockone.job.core.SparkMLFrame
    * ...
    * 1. 本地IDEA在main方法里开发调式模式
    * val sparkml = SparkMLFrame(master="local[2]")
    * 2. local mode1 with spark-submit
    * val sparkml = SparkMLFrame()
    * $ spark-submit... 或者$ spark-submit --master spark://sparkmaster:7077 ... 或者$ spark-submit --master local[2] ...
    * 3. val sparkml = SparkMLFrame("spark-ml", "spark://sparkmaster:7077", SparkMLFrame.defaultRemoteSetting(1,1,1,"4g","10g"), "/opt/spark/share/")
    * 4. $spark-submit --master yarn --deploy-mode [cluster|client]...
    * val sparkml = SparkMLFrame("CaffeOnSparkHelper", null, SparkMLFrame.defaultRemoteSetting(1,1,1,"4g","10g"), "/opt/spark/share/")
    * ...
    *
    * @param appName
    * @param master
    * @param setting
    * @return
    */
  def apply(appName: String = "spark-ml",
            master: String = null,
            setting: Map[String, String] = null,
            streamingMilliSecondsDuration: Long = 0) = {
    new SparkWrapper(appName, master, setting, streamingMilliSecondsDuration).init()
  }

  def defaultRemoteSetting(sparkWorkerInstances: Int = 1, coresPerWorker: Int = 1, driverCores: Int = 1
                           , driverMemory: String = "2g", executorMemory: String = "4g"): Map[String, String] = {
    val totalCores: String = Integer.toString(sparkWorkerInstances * coresPerWorker)
    this.remoteSetting += "spark.serialize" -> "org.apache.spark.serializer.KryoSerializer"
    this.remoteSetting += "spark.driver.cores" -> Integer.toString(driverCores)
    this.remoteSetting += "spark.cores.max" -> totalCores
    this.remoteSetting += "spark.task.cpus" -> coresPerWorker.toString
    this.remoteSetting += "spark.driver.memory" -> driverMemory
    this.remoteSetting += "spark.executor.memory" -> executorMemory
    this.remoteSetting += "spark.executor.cores" -> Integer.toString(coresPerWorker)
    this.remoteSetting += "spark.default.parallelism" -> totalCores  //RDD默认分区数, 不应超过spark.task.cpus的值
    this.remoteSetting += "spark.sql.shuffle.partitions" -> totalCores
    this.remoteSetting += "spark.eventLog.enabled" -> "true"
    this.remoteSetting += "spark.files.overwrite" -> "true"
    this.remoteSetting += "spark.driver.extraLibraryPath" -> System.getenv("LD_LIBRARY_PATH")
    this.remoteSetting += "spark.executorEnv.LD_LIBRARY_PATH" -> System.getenv("LD_LIBRARY_PATH")
    this.remoteSetting += "spark.scheduler.mode" -> "FAIR"
    this.remoteSetting += "spark.driver.allowMultipleContexts" -> "true"
    this.remoteSetting += "spark.shuffle.service.enabled" -> "true"
    this.remoteSetting += "spark.dynamicAllocation.enabled" -> "true"
    this.remoteSetting
  }

  /**
    * 例子:
    * val sparkml = SparkMLFrame()
    * val df = SparkMLFrame.loadMySQLDF(sparkml.getSQLContext(), db_name="tushare", table_name="stock_basics").load().filter("1=1")
    * df.show(100)
    *
    * @param db_host
    * @param db_port
    * @param user_name
    * @param passwd
    * @param db_name
    * @param table_name
    * @return
    */
  def loadMySQLDF(sqlContext: SQLContext,
                  db_host: String = "docker_mysql",
                  db_port: String = "3306",
                  user_name: String = "root",
                  passwd: String = "123456",
                  db_name: String = "",
                  table_name: String = ""): DataFrameReader = {
    val dfReader = sqlContext.read.format("jdbc")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("url", "jdbc:mysql://" + db_host + ":" + db_port + "/" + db_name + "?characterEncoding=utf8")
      .option("user", user_name)
      .option("password", passwd)
      .option("dbtable", table_name)
    dfReader
  }

  /**
    * 例子:
    * val df = ...
    * SparkMLFrame.saveDFtoMySQL(df, db_name="tushare", save_to_table="stocks")
    *
    * @param db_host
    * @param db_port
    * @param user_name
    * @param passwd
    * @param db_name
    * @param save_to_table
    * @return
    */
  def saveDFtoMySQL(df: DataFrame,
                    db_host: String = "docker_mysql",
                    db_port: String = "3306",
                    user_name: String = "root",
                    passwd: String = "123456",
                    save_mode: String = "overwrite",
                    db_name: String = "",
                    save_to_table: String = ""): Unit = {
    val props = new Properties()
    props.put("user", user_name)
    props.put("password", passwd)
    df.write
      .mode(save_mode)
      .jdbc(url = "jdbc:mysql://" + db_host + ":" + db_port + "/" + db_name + "?characterEncoding=utf8",
        table = save_to_table,
        connectionProperties = props)
  }

  /**
    * 例子:
    * ...
    * val sparkml = SparkMLFrame()
    * val df = SparkMLFrame.loadCassandraToDF(sparkml.getSQLContext(), "cassandra1", "testdb", "employee")
    * df.show
    * ...
    *
    * @param sqlContext
    * @param host     , default: "cassandra1"
    * @param keyspace , e.g. "testdb"
    * @param table    , e.g. "employee"
    * @return
    */
  def loadCassandraToDF(sqlContext: SQLContext, host: String, keyspace: String, table: String): DataFrameReader = {
    scala.collection.GenTraversable
    sqlContext.setConf("spark.cassandra.connection.host", host)
    sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> keyspace,
        "table" -> table,
        "cluster" -> "Test Cluster",
        "spark.cassandra.input.split.size_in_mb" -> "48"
      ))
  }

}
