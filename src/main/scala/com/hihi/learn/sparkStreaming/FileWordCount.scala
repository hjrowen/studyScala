package com.hihi.learn.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FileWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("FileWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.textFileStream("file:///E:/data/sparkDemo/emp/output/1980")
    val words = lines.flatMap(_.split(" ")).mapPartitions(iter => {
      for (it <- iter)
        yield (it, 1)
    }).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
