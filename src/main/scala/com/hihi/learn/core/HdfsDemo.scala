package com.hihi.learn.core

import org.apache.spark.{SparkConf, SparkContext}

object HdfsDemo {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("EmpDemo").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    val file = sc.textFile("hdfs://hadoop001:9000/data/*.txt")
      .flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_).collect().foreach(println)
  }
}
