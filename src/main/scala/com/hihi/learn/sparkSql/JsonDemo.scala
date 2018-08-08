package com.hihi.learn.sparkSql

import org.apache.spark.sql.SparkSession

object JsonDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("JsonDemo.")
      .master("local")
      .getOrCreate()

    // 1、从一个有dirty data的json文件读取数据，spark会自动兼容
//    {"name":"zhangsan", "gender":"F", "height":160, "job":"program"}
//    {"name":"lisi", "gender":"M", "height":175, "age":30}
//    {"name":"wangwu", "age":33, "gender":"M", "height":180.3}
    val jsonDF = spark.read.format("json").load("E:\\spark-branch-2.2\\examples\\src\\main\\resources\\people.json")
    jsonDF.show()
//    +----+------+------+-------+--------+
//    | age|gender|height|    job|    name|
//    +----+------+------+-------+--------+
//    |null|     F| 160.0|program|zhangsan|
//    |  30|     M| 175.0|   null|    lisi|
//    |  33|     M| 180.3|   null|  wangwu|
//    +----+------+------+-------+--------+

    jsonDF.printSchema()
//    root
//    |-- age: long (nullable = true)
//    |-- gender: string (nullable = true)
//    |-- height: double (nullable = true)
//    |-- job: string (nullable = true)
//    |-- name: string (nullable = true)

    spark.stop()
  }
}
