package com.hihi.learn.sparkSql
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object MySqlDemo {

  val url = "jdbc:mysql://hadoop001:3306"
  val table = "soctt.dept"
  val dBuser = "root"
  val dBpwd = "123456"
  val prop = new Properties()
  prop.setProperty("user", "root")
  prop.setProperty("password", "123456")
  case class DeptRecord(deptno: Int, dname: String, loc: String)

  def write(df: DataFrame, table: String) : Unit = {
    df.write.mode(SaveMode.Append).jdbc(url, table, prop)
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Mysql Demo.")
      .master("local")
      .getOrCreate()

    readFromMysql(spark)
    writeToMysql(spark)
    readFromMysql(spark)

    spark.stop()
  }

  // 从mysql读取数据
  def readFromMysql(spark: SparkSession): Unit = {
    val jdbcDF  = spark.read.format("jdbc")
      .option("url", url)
      .option("dbtable", table)
      .option("user", dBuser)
      .option("password", dBpwd).load()

    jdbcDF.show()
  }

  // 向mysql写数据
  def writeToMysql(spark: SparkSession): Unit = {
    val arr = Array(
      DeptRecord(50, "AA", "aa"),
      DeptRecord(60, "BB", "bb")
    )
    val jdbcDF = spark.createDataFrame(arr)
    jdbcDF.write.mode(SaveMode.Append).jdbc(url, table, prop)
  }
}
