package com.hihi.learn.sparkStreaming

import org.apache.spark._
import org.apache.spark.streaming._

object NetWordCountApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.socketTextStream("hadoop001", 9999)
    //val lines = ssc.rawSocketStream("hadoop001", 9999)
    val words = lines.flatMap(_.split(" ")).mapPartitions(iter => {
      for (it <- iter)
        yield (it, 1)
    }).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }
}
