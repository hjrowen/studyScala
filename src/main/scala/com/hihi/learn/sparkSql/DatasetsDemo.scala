package com.hihi.learn.sparkSql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer


object DatasetsDemo {

  case class Person(name: String, age: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DatasetsDemo")
      .master("local")
      .getOrCreate()

    // Creating Datasets
    import spark.implicits._
    val personDS = Seq(Person("Hihi", 22), Person("Tom", 11)).toDS
    personDS.show

    val personDS2 = spark.read.format("json").load("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\people.json").as[Person]
    personDS2.show

    spark.stop()
  }
}
