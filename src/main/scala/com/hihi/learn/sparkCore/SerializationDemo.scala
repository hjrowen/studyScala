package com.hihi.learn.sparkCore

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

case class Student(id: String, name: String, age: Int, gender: String)

object SerializationDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SerializationDemo")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[Student])) // 将自定义的类注册到Kryo
    val sc = new SparkContext(conf)

    val stduentArr = new ArrayBuffer[Student]()
    for (i <- 1 to 1000000) {
      stduentArr += (Student(i + "", i + "a", 10, "male"))
    }
    val JavaSerialization = sc.parallelize(stduentArr)
    JavaSerialization.persist(StorageLevel.MEMORY_ONLY_SER).count()

    while(true) {
      Thread.sleep(10000)
    }

    sc.stop()
  }
}
