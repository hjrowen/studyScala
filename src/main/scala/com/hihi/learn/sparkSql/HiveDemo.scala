package com.hihi.learn.sparkSql
import java.io.File

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

case class Record(date: String, loginfo: String, cnt: String)

object HiveDemo {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    // TODO 下列操作的参数都应该改为从外部传入，尽量避免后续需求变更而修改代码
    val df = sql("select max(date), CONCAT(hostname,'_',servicename,'_',logtype),loginfo,count(loginfo) from onlineloganalysis.log_info where date='20180429' group by hostname,servicename,logtype,loginfo")
      .toDF("date","schema_info","loginfo", "cnt")
    MySqlDemo.write(df, "onlineloganalysis.hive_frequency_word")
  }
}
