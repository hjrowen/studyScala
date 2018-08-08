package com.hihi.learn.sparkSql

import com.hihi.learn.sparkSql.DatasetsDemo.Person
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object DataFrameDemo {
  case class Person(name: String, age: Long)
  def main(args: Array[String]): Unit = {
    // 创建SparkSession，此为spark SQL的入口
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[2]")
      .getOrCreate()
    import spark.implicits._
    //Creating DataFrames
//    val df = spark.read.format("json").load("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\people.json")
    //df.show()
    /*
    +----+-------+
    | age|   name|
    +----+-------+
    |null|Michael|
    |  30|   Andy|
    |  19| Justin|
    +----+-------+
    */

    // 使用算子操作DataFrame
    //UntypedDatasetOperations(df)

    // 使用SQL编程方式
//    RunningSqlQueriesProgrammatically(spark, df)
    // Interoperating with RDDs
//    val peopleDF = spark.sparkContext.textFile("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\people.txt")
//      .mapPartitions(its => {
//        var arrPerson = ArrayBuffer[Person]()
//        for (it <- its) {
//          val arr = it.split(",")
//          arrPerson += Person(arr(0), arr(1).trim.toLong)
//        }
//        arrPerson.toIterator
//      })
//      .toDF().show
//
//    // Programmatically Specifying the Schema
//    val personDS4 = spark.sparkContext.textFile("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\people.txt")
//    val schemaString = "name age"
//    val fields = schemaString.split(" ").map(StructField(_, StringType, nullable = true))
//    val schema = StructType(fields)
//
//    val rowRDD = personDS4
//      .map(_.split(","))
//      .map(attributes => Row(attributes(0), attributes(1).trim))
//
//    val peopleDF2 = spark.createDataFrame(rowRDD, schema)
//    peopleDF2.show()
//    peopleDF2.write.format("orc").mode("overwrtie").save("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\people.orc")
//    spark.stop()
    val peopleOrcDf = spark.read.format("orc").load("E:\\\\spark-branch-2.2\\\\examples\\\\src\\\\main\\\\resources\\\\people.orc")
    peopleOrcDf.show()

  }

  def UntypedDatasetOperations(df:DataFrame): Unit = {

    // 打印Schema
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // 查询 name
    df.select("name").show
    // +-------+
    // |   name|
    // +-------+
    // |Michael|
    // |   Andy|
    // | Justin|
    // +-------+

    // 查询age > 21的数据
    df.filter("age > 21").show
    // +---+----+
    // |age|name|
    // +---+----+
    // | 30|Andy|
    // +---+----+

    // 使用分组
    df.groupBy("age").count.show()
    // +----+-----+
    // | age|count|
    // +----+-----+
    // |  19|    1|
    // |null|    1|
    // |  30|    1|
    // +----+-----+
  }

  def RunningSqlQueriesProgrammatically(spark: SparkSession, df: DataFrame) : Unit = {
    // 注册一张临时表
    df.createOrReplaceTempView("people")

    spark.sql("select name, age from people").show()
    // +----+-------+
    // | age|   name|
    // +----+-------+
    // |null|Michael|
    // |  30|   Andy|
    // |  19| Justin|
    // +----+-------+

    spark.sql("select name, age from people where name = 'Andy'").show
    // +----+---+
    // |name|age|
    // +----+---+
    // |Andy| 30|
    // +----+---+
  }

}
