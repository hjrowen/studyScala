package com.hihi.learn

/**
  *  声明、定义变量
  */

object VariableDemo {
  def main(args: Array[String]): Unit = {
    // 定义一个变量，吧变量名在前，类型在后。
    var i: Int = 100;

    //Scala编译器会自动推断变量的类型，必要的时候可以指定类型。
    var i2 = 100
    var s: String = "Hello Scala"
    var s2 = "Hello World"

    // 使用val修饰的变量不可变，相当于java里用final修饰的变量，scala中推荐使用val。
    val s3 = "使用val修饰的变量不可变"

  }
}
