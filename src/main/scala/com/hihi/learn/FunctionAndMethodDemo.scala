package com.hihi.learn

object FunctionAndMethodDemo {

  // 定义一个函数
  val add : (Int, Int) => Int = {
    (x, y) => x + y
  }

  // 同样是定义一个函数
  val dif = (x: Int, y: Int) => (x - y)

  // 定义一个方法
  def  mult(x: Int, y: Int) : Int = {
    x * y
  }

  // 定义一个方法
  def Calculate(func:(Int, Int) => Int, x: Int, y: Int) : Int = {
    func(x,y)
  }

  val Calculate2 =  (func:(Int, Int) => Int, x: Int, y: Int) => {
    func(x,y).toString
  }


  def main(args: Array[String]): Unit = {
    println(Calculate(add,3,2))
    println(Calculate(mult,3,2))
    println(Calculate2(dif,1,2))
    println(Calculate2(mult,6,2))

    // 调用函数，传入匿名函数
    println(Calculate2((x,y) => x / y, 10,5))
    // 调用方法，传入匿名函数
    println(Calculate2((x,y) => {println("hello world.");x / y}, 10,2))

    // 将一个方法强转为函数类型
    val f = mult _
  }

}
