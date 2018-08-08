package com.hihi.learn.ObjectOriented

// scala中不存在public关键字，源文件中可以包含多个类，这些类都具有公有可见性
// 定义一个抽象类
abstract class Animal {
  val age:Int = 0
  val weight:Int = 0

  // 抽象类中的函数能有默认实现
  def cry(): Unit = println("This a Animal.")
}

// 定义一个类型，继承抽象类Animal，在Scala中和Java一样都是使用extends关键字进行继承。
class Cat extends Animal {
  // 重写父类方法。
  // 能访问伴生对象中的私有成员变量
  override def cry(): Unit = println(s"This a cat.${Cat.voice}")
}

// Cat的伴生对象，object定义的类，成为伴生对象，也是单例对象。
object Cat {
  private val voice = "喵喵喵"
  def apply(): Cat = {
    println("Cat apply.")
    new Cat()
  }

  // 重载apply()
  def apply(info:String): Cat = {
    println(info)
    new Cat()
  }

  def main(args: Array[String]): Unit = {
    // 这里相当于调用Cat伴生对象中apply()方法。伴生对象中的apply()方法，类似c++中重载()操作符
    val cat = Cat().cry()
    val cat2 = Cat("调用重载的构造方法").cry()
  }
}
