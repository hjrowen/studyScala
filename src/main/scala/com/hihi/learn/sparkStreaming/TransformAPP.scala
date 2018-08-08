package com.hihi.learn.sparkStreaming

import org.apache.spark._
import org.apache.spark.streaming._

object TransformAPP {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("TransformAPP")
    val ssc = new StreamingContext(conf, Seconds(5))


    val blakListRDD = ssc.sparkContext.parallelize(List(("zs",true), ("ls", true), ("ww", false)))
    val broadcastVar = ssc.sparkContext.broadcast(blakListRDD.collect)

    val lines = ssc.socketTextStream("hadoop001", 9999)
    // "zs,20180806", "ls,20180806", "ww,20180806"
    //val lines = ssc.rawSocketStream("hadoop001", 9999)
    val words = lines.mapPartitions( it => {
      for (x <- it)
        yield (x.split(",")(0),x) // (zs  "zs,20180806")
    }).transform(rdd => {
      rdd.leftOuterJoin(blakListRDD)  // (zs :<"zs,20180806",true>)
        .filter(_._2._2.getOrElse(false) != true)
        .map(x=>x._2._1)
    }).print()


    ssc.start()
    ssc.awaitTermination()
  }
}
