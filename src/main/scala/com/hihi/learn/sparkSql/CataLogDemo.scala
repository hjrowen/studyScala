package com.hihi.learn.sparkSql

import org.apache.spark.sql.SparkSession

object CataLogDemo {
  // 在linux环境下使用spark-shell掩饰
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("DatasetsDemo")
      .master("local[1]")
      .getOrCreate()

    spark.catalog.listDatabases().show() // 获取当前的database
    spark.catalog.listTables("topn_demo").show() //获取topn_demo下的所有表
    spark.catalog.listColumns("topn_demo", "user_click").show() //获取topn_demo.user_click的列信息
    spark.catalog.cacheTable("topn_demo.user_click") // 缓存一张表的数据，spark 2.2.0 中是lazy的
    spark.catalog.isCached("topn_demo.user_click")
    spark.table("topn_demo.user_click").show // 执行后才是真正的缓存
    spark.sql("cache table topn_demo.user_click") // 立刻缓存 storage level == 	Memory Deserialized 1x Replicated
    spark.catalog.uncacheTable("topn_demo.user_click") // 释放一张表的缓存

    spark.stop()
  }
}
