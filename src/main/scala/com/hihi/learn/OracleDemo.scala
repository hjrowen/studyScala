package com.hihi.learn

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object OracleDemo {
  def selectOrc(): Unit = {
    Class.forName("oracle.jdbc.driver.OracleDriver")
    val ss = SparkSession.builder().appName("testRDDMethod").master("local").getOrCreate()
//    var theConf = new SparkConf().setAppName("testRDDMethod").setMaster("local")
//    var theSC = new SparkContext(theConf)
//    var theSC2 = new SQLContext(theSC)
//    var theJdbcDF = ss.read.jdbc("jdbc",Map("url"->"jdbc:oracle:thin:用户/密码@//ip地址:端口/实例名",
//      "dbtable" -> "(select * from tab) a","driver"->"oracle.jdbc.driver.OracleDriver"))
//    theJdbcDF.registerTempTable("myuser")
//    var theDT = theSC2.sql("select * from myuser")
//    theDT.registerTempTable("tempsum")
    val jdbcMapv = Map("url" -> "jdbc:oracle:thin:@//YOS-01609031835:1521/HJR",
      "user" -> "scott",
      "password" -> "123",
      "dbtable" -> "spark_test",
      "driver" -> "oracle.jdbc.driver.OracleDriver")
    val jdbcDFv = ss.read.options(jdbcMapv).format("jdbc").load
    jdbcDFv.createTempView("spark_test")
    ss.sql("select * from spark_test").show(false)

    //val count = jdbcDFv.where("ename = 'SMITH'").limit(1).count()
    //ss.sql("insert into spark_test values('hihi')")
    //ss.sql("select * from emp").show(false)
    ss.stop()
  }

  def main(args: Array[String]): Unit = {
    selectOrc()
  }
}
