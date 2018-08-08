package com.hihi.learn.empDEMO

import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

  object EmpDemo {

    // 自定义一个类，继承MultipleTextOutputFormat
    class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[NullWritable, Text] {
      override def generateFileNameForKeyValue(key: NullWritable, value: Text, name: String): String ={
        val str = value.toString
        val year = str.split("\t")(4).substring(0, 4)
        val cn = str.substring(str.length()-1, str.length())
        value.set(str.substring(0,str.length()-1))
        "year=" + year + "/" + cn + "-" + year + ".txt"
      }
    }
    def main(args: Array[String]): Unit = {
      if (args.length < 3) {
        System.err.println("Usage: EmpDemo <inputPath> <outputPath>")
        System.exit(1)
      }
      val sparkConf = new SparkConf().setAppName("EmpDemo").setMaster("yarn-cluster")
      val sc = new SparkContext(sparkConf)
      val file = sc.textFile(args(0)).mapPartitions(iter => {
        for (it <- iter; arr = it.split("\t"))
          yield (if ("".equals(arr(4))) "NULL"
            // 第四个字段取出来，1990/1/1
          else arr(4).substring(0, 4), it+args(2)) // args(2) 为什么要加上去
      }).partitionBy(new HashPartitioner(1)).mapPartitions(iter => {
          val value = new Text()
          iter.map(it => {
            value.set(it._2.toString)
            (NullWritable.get(), value)
          })
        })
      RDD.rddToPairRDDFunctions(file)(implicitly[ClassTag[NullWritable]],
        implicitly[ClassTag[Text]], null)
        .saveAsHadoopFile(args(1),classOf[NullWritable],
          classOf[Text], classOf[RDDMultipleTextOutputFormat])
      sc.stop()
      // 剩下的交给shell移动到正式目录，考虑重跑，所以要用shell移动到正式目录
    }
  }