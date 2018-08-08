package com.hihi.learn.implicitDemo

class Man {
  def run: Unit ={
    println("runing...")
  }
}

class SuperMan(ma:Man) {
  def fly: Unit = {
    println("flying...")
  }
}

object SuperMan {
  def main(args: Array[String]): Unit = {
    val man = new Man
    new SuperMan(man).fly
    import MyPredef1._
    man.fly
  }
}
