//package com.hihi.learn.Match
//import scala.util.Random
//
//abstract class Animal extends flyable {
//  def print {
//    printf("This is %s .\n" ,this.getClass.getSimpleName())
//  }
//}
//
//trait flyable {
//  var flyable: Boolean = false
//}
//
///**
//  *在Scala中样例类是一中特殊的类，可用于模式匹配。
//  * case class是多例的，后面要跟构造参数，case object是单例的
//  */
//case object Cat extends Animal
//case class Dog() extends Animal
//case class Bird() extends Animal {
//  flyable = true
//}
//
//object CaseClass extends App {
//  val animalArr = Array[Animal](Cat, new Dog, new Bird)
//  val animal = animalArr(Random.nextInt(animalArr.length))
//
//  animal match {
//    case Cat =>
//      Cat.print
//    case dog: Dog =>
//      dog.print
//    //case bird: Bird =>
//    // 模式匹配中还可以加入条件。
//    case animal: Animal if (animal.flyable) =>
//      animal.print
//    case _ =>
//      println("Nothing....")
//  }
//}
