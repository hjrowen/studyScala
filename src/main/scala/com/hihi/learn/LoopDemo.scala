package com.hihi.learn

/**
  * 循环
  */
object LoopDemo {
  def main(args: Array[String]): Unit = {
    // for (i <- 表达式/数组/集合)
    // 1 to 10 相等于Int.to(Int)，在一个匿名空间构造值为1的Int变量，调用to(Inf)方法
    for (i <- 0 to 10) println(i)  // 左闭右闭
    for (i <- 0 until 10) println(i) // 左闭右开区间
    for (i <- Range(0,10)) println(i) // 左闭右开区间

    // 双重循环，循环构造器中还能加上条件表达式，yield是关键字，可以把再构造一个新的数组
    var arr = for (i <- 1 to 9; j <- 1 to 9 if i == j ) yield i + j
    println(arr.toBuffer)
  }
}
