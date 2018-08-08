package com.hihi.learn.sparkSql

import org.apache.spark.sql.{SaveMode, SparkSession}

object DataSetVsDataFrame {

  case class Car(year: Int, make: String, model: String, comment: String, blank: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DatasetsDemo")
      .master("local[1]")
      .getOrCreate()

    import spark.implicits._

    //writeCsvFile(spark)
    //readCsvFile(spark)
    DatasetVsDataFrame(spark)
    spark.stop()
  }

  def DatasetVsDataFrame(spark: SparkSession): Unit = {
    import spark.implicits._
    val carsDF = spark.read.format("csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("E:\\spark-branch-2.2\\sql\\core\\src\\test\\resources\\test-data\\cars.csv")
//    carsDF.printSchema()
//    carsDF.show()

    val carsDS = carsDF.as[Car]
    val filterDS = carsDS.filter(_.year == 1997)  // 运行时安全
    val whereDF = carsDF.where("year = 1997")    // 存在运行时异常的情况

    println(filterDS.queryExecution.optimizedPlan.numberedTreeString) // DS能暴露更多信息，信息暴露得越多，优化的效果就越好
    println("------------------------------------------------")
    println(whereDF.queryExecution.optimizedPlan.numberedTreeString)
  }

  def readCsvFile(spark: SparkSession): Unit = {
    import spark.implicits._
    val carsDF = spark.read.format("csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("E:\\spark-branch-2.2\\sql\\core\\src\\test\\resources\\test-data\\cars.csv")
    //carsDF.show()
    //carsDF.filter("year = 1997").show()

    val carsDS = carsDF.as[Car]
    carsDS.printSchema()
    carsDS.show(false)
    carsDS.filter(!_.year.equals(1997)).show()
    carsDS.select()

    carsDF.write.mode(SaveMode.Overwrite).format("json").save("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\cars.json")

    val carsDF2 = spark.read.format("json")
      .load("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\cars.json")
    carsDF2.printSchema()
    carsDF2.show()
  }

  def writeCsvFile(spark: SparkSession): Unit = {
    import spark.implicits._

    val peopleDF = spark.read.format("json").load("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\people.json")
    peopleDF.printSchema()
    peopleDF.show()
    peopleDF.write.mode(SaveMode.Overwrite).csv("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\people.csv")


    val peopleDF2 = spark.read.format("csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\people.csv")
    peopleDF2.printSchema()
    peopleDF2.show()
  }

}
