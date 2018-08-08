package com.hihi.learn.ObjectOriented

/**
  *每个类都有主构造器，主构造器的参数直接放置类名后面，与类交织在一起
  */
class Person (val name:String, val age:Int){ // 这里使用val 休息的成员变量，表示是私有属性的

  var gender: Char = 'M'
  println("主构造器")
  def this(name:String, age:Int, gender: Char) {
    this(name,age)   // 辅助构造器中，需要先调用主构造器
    this.gender = gender
    println("辅助构造器")
  }

  def print(): Unit = {
    println(s"name is ${name}; age is ${age}; gender is :${gender}")
  }
}

// 超类的构造语法
class Student(name:String, age:Int) extends Person(name, age) {
  override def print(): Unit ={
    System.out.print("I'm a student.")
    super.print()
  }
}

object Person {
  def main(args: Array[String]): Unit = {
    val p1 = new Person("xiao ming", 18)
    p1.print()
    val p2 = new Person("xiao hong", 12 , 'F')
    p2.print()
    val p3 = new Student("xiao nie", 11)
    p3.print()
  }
}
