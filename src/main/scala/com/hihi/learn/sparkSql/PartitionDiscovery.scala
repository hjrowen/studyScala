package com.hihi.learn.sparkSql

import org.apache.spark.sql.SparkSession

object PartitionDiscovery {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("PartitionDiscovery")
      .master("local")
      .getOrCreate()

//     文件内容如下
//    {"name":"Michael"}
//    {"name":"Andy", "age":30}
//    {"name":"Justin", "age":19}

    // 文件实际路径：E:\MyData\people\country=China，spark会根据目录自动探索分区
    val peopleDF = spark.read.format("json").load("E:\\MyData\\people_json_GZIP")
    //peopleDF.write.option("compression", "gzip").format("json").mode("overwrite").save("E:\\MyData\\people_json_GZIP")
    peopleDF.show()
//    +----+-------+-------+
//    | age|   name|country|
//    +----+-------+-------+
//    |null|Michael|  China|
//    |  30|   Andy|  China|
//    |  19| Justin|  China|
//    +----+-------+-------+
  }
}
