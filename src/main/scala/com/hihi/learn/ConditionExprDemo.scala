package com.hihi.learn

/**
  * 条件表达式
  */
object ConditionExprDemo {
  def main(args: Array[String]): Unit = {
    val a = 100
    val b = 200
    // 比较a、b的大小，获取较大的值，但不建议用这种方式
    var max = 0
    if (a > b) {
      max = a
    } else {
      max = b
    }

    // scala中可以直接将使用如下语法，推荐使用。
    var max2 = if (a > b) a else b
    println(max + "," + max2)

    // 同时支持块表达式
    val c = 300
    var max3 = if (a > b){
      if (a > c) a else c
    } else {
      if (b > c) b else c
    }
    println(max3)
  }
}
