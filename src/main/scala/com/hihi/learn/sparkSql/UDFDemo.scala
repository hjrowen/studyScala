package com.hihi.learn.sparkSql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

case class Person(name: String, hobbies: String)

class MyUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =  StructType(Array(StructField("input", StringType, true)))

  override def bufferSchema: StructType = StructType(Array(StructField("count", StringType, true)))

  override def dataType: DataType = StringType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {buffer(0) = ""}

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if ("".equals(buffer.getAs[String](0))) {
      buffer(0) = input.getAs[String](0)
      return
    }
    buffer(0) = buffer.getAs[String](0) + "," + input.getAs[String](0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if ("".equals(buffer1.getAs[String](0))) {
      buffer1(0) = buffer2(0)
      return
    }
    buffer1(0) = buffer1.getAs[String](0) + "," + buffer2.getAs[String](0)
  }

  override def evaluate(buffer: Row): Any = {
    println("evaluate")
    println(buffer.getAs[String](0))
    buffer.getAs[String](0)
  }
}

object UDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("UDF Demo.")
      .master("local")
      .getOrCreate()

    spark.udf.register("getFieldCount", (field: String, regex: String) => field.split(regex).length)
    spark.udf.register("fieldCat", new MyUDAF)
//    strLenDemo(spark)
//    hobbiesConut(spark)
    ipCount(spark)

    spark.stop()
  }

  def ipCount(spark: SparkSession): Unit = {
    val schemaString = "name ip"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // 获取数据
    val logIpRDD = spark.sparkContext.textFile("E:\\base-demo1\\sparkSql\\login_ip.txt")
      .mapPartitions(iter => {
        var arr: Array[String] = null
        for (it <- iter;arr = it.split(" "))
          yield Row(arr(0), arr(1))
      })
    val logIpDF = spark.createDataFrame(logIpRDD, schema)
    logIpDF.createOrReplaceTempView("logIp")

//    spark.sql("select name ,fieldCat(ip) as ips " +
//      "from logIp group by name").createTempView("logIp2")
    //spark.sql("select name, ips, getFieldCount(ips, ',') count from logIp2").show()
      spark.sql("select name ,fieldCat(ip) as ips, getFieldCount(fieldCat(ip), ',') as count " +
        "from logIp group by name").show(false)
  }

  def hobbiesConut(spark: SparkSession): Unit = {
    val schemaString = "name hobbies"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    // 获取数据
    val hobbiesRDD = spark.sparkContext.textFile("E:\\base-demo1\\sparkSql\\hobbies.txt")
      .mapPartitions(iter => {
        var arr: Array[String] = null
        for (it <- iter;arr = it.split(" "))
          yield Row(arr(0), arr(1))
      })
    val hobbiesDF = spark.createDataFrame(hobbiesRDD, schema).createOrReplaceTempView("hobby")

    // 注册 UDF 函数
    spark.udf.register("getFieldCount", (field: String, regex: String) => field.split(regex).length)
    spark.sql("select name, hobbies, getFieldCount(hobbies, ',') from hobby").show(false)
  }

  def strLenDemo(spark: SparkSession): Unit = {
    val arr = Array("hello", "java", "cpp", "scala")

    spark.udf.register("strLen", (str: String) => str.length)
    import spark.implicits._
    val myDF = spark.sparkContext.parallelize(arr).toDF("name").createTempView("TEST")

    spark.sql("select name, strLen(name) from TEST").show()
  }

}
