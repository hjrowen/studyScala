package com.hihi.learn.Match

import scala.util.Random

abstract class Animal extends flyable {
  def print {
    printf("This is %s .\n" ,this.getClass.getSimpleName())
  }
}

trait flyable {
  var flyable: Boolean = false
}

class Cat extends Animal
class Dog extends Animal
class Bird extends Animal {
  flyable = true
}

/**
  * 类型匹配
  * scala中的类型匹配可谓十分灵活，而且使用也十分简单。
  */
object CaseType extends App{
  val animalArr = Array[Animal](new Cat(), new Dog, new Bird)
  val animal = animalArr(Random.nextInt(animalArr.length))

  animal match {
    case cat: Cat =>
      cat.print
    case dog: Dog =>
      dog.print
    //case bird: Bird =>
    // 模式匹配中还可以加入条件。
    case animal: Animal if (animal.flyable) =>
      animal.print
    case _ =>
      println("Nothing....")
  }
}
