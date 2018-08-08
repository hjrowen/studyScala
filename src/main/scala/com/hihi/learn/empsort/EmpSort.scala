package com.hihi.learn.empsort

import org.apache.spark.{SparkConf, SparkContext}

/**
empno                   int
ename                   string
job                     string
mgr                     int
hiredate                string
sal                     double
comm                    double
deptno                  int
  */
class Emp extends Ordered[Emp] with  Serializable {
  var empno:String = _
  var ename:String = _
  var job:String = _
  var mgr:String = _
  var hiredate:String = _
  var sal:Float = _
  var comm:Float = _
  var deptno:String = _

  def this(info:String) {
    this()
    val splitInfo = info.split("\t")
    if (8 == splitInfo.length) {
      empno = splitInfo(0)
      ename = splitInfo(1)
      job = splitInfo(2)
      mgr = splitInfo(3)
      hiredate = if ("".equals(splitInfo(4)))
        splitInfo(4)
      else
        "unknow"
      sal = if ("".equals(splitInfo(5)))
        0.0f
      else
        splitInfo(5).toFloat
      comm = if ("".equals(splitInfo(6)))
        0.0f
      else
        splitInfo(6).toFloat
      deptno = splitInfo(7)
    }
  }

  def show = {
    printf("ename = %s,sal = %f,comm = %f\n",ename, sal, comm)
  }

  override def compare(that: Emp): Int = {
    if (sal == that.sal)
      (comm - that.comm).toInt
    else
      (sal - that.sal).toInt
  }
}

object EmpSort {
  def main(args: Array[String]) {
    println(Runtime.getRuntime.maxMemory())
    return
    val conf = new SparkConf()
      .setAppName("FirstValueApp")
      .setMaster("local[*]")
      .set("spark.driver.memory", "4g")
      .set("spark.executor.memory", "4g")
      //.setMaster("yarn")
    val sc = new SparkContext(conf)
    //val data = sc.textFile("C:/apollo_update.log")
//    val data = sc.textFile("hdfs://hadoop001:9000/user/hive/warehouse/soctt.db/emp/emp.txt")
//    val emps = data.map(new Emp(_))
//    // 二次排序
//    emps.sortBy(x => x,false).collect().foreach(_.show)
//
//    // 求comm为空的总数
//    val accum = sc.longAccumulator("My Accumulator")
//    emps.foreach(x=> {
//      if (0 == x.comm) accum.add(1)
//    })
//    println(accum.value)
//
//    val data2 = sc.parallelize(Array(
//      (1, "one"),
//      (2, "two"),
//      (3, "three"),
//      (4, "four")
//    ))

//    val data2 = (Array(
//      (1, "one"),
//      (2, "two"),
//      (3, "three"),
//      (4, "four")
//    ))
//    //val data3 = sc.broadcast(List(
////    val data3 = sc.parallelize(List(
////      ("A1", 1),
////      ("A2", 2),
////      ("A3", 3),
////      ("B4", 4),
////      ("B3", 3),
////      ("B2", 2),
////      ("C4", 4)
////    ))
////    val broad = sc.broadcast(data2)
////
////    data3.mapPartitions(iter => {
////      val myBroad = broad.value.toMap
////      for ( (key,value) <- iter
////        if (myBroad.contains(value)))
////        yield (key, myBroad.getOrElse(value, ""))
////    }).collect().foreach(println(_))
////
      while (true) {
        Thread.sleep(10000)
      }
////    data2.join(x=> {
////
////    }).foreach(println)
    sc.stop()
  }

}






















