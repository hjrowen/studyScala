package com.hihi.learn.Match

import scala.util.Random
import scala.util.matching.Regex.Match

/**
  * 字符串匹配
  * scala中的case匹配成功后，就不会再进行匹配，自动break。
  */
object CaseString {
  def main(args: Array[String]): Unit = {
    val strArr = Array[String]("XiaoMing", "XiaoHong", "NieNie")
    val name = strArr(Random.nextInt(strArr.length))

    name match {
      case "XiaoMing" =>
        println("我是小明。")
      case "XiaoHong" =>
        println("我是小红。")
      case "NieNie" =>
        println("我是捏捏。")
      case _ =>
        println("什么都不是。")
    }
  }
}
