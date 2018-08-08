package com.hihi.learn.sparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object StatefulWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    // 如果使用了stateful的算子，必须要设置checkpoint
    // 在生产环境中，建议大家把checkpoint设置到HDFS的某个文件夹中
    ssc.checkpoint(".")

    val lines = ssc.socketTextStream("hadoop001", 9999)
    //val lines = ssc.rawSocketStream("hadoop001", 9999)
    val words = lines.flatMap(_.split(" ")).mapPartitions(iter => {
      for (it <- iter)
        yield (it, 1)
    })
    //val state = words.updateStateByKey[Int](updateFunction(_,_))
    val state = words.updateStateByKey((currentValues:Seq[Int], preValues:Option[Int]) =>
      Some(currentValues.sum + preValues.getOrElse(0))
    )
    //mapWithState
    //val initialRDD = ssc.sparkContext.parallelize(List(("hello", 1), ("world", 1)))
   // val state = words.mapWithState(StateSpec.function(mappingFunc))
    state.print()
    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 把当前的数据去更新已有的或者是老的数据
    * @param currentValues  当前的
    * @param preValues  老的
    * @return
    */
  def updateFunction(currentValues: Seq[Int], preValues: Option[Int]): Option[Int] = {
    val current = currentValues.sum
    val pre = preValues.getOrElse(0)

    Some(current + pre)
  }

  val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
    val sum = one.getOrElse(0) + state.getOption.getOrElse(0)
    val output = (word, sum)
    state.update(sum)
    output
  }
}
