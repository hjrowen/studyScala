package com.hihi.learn.Match

/**
  * 数据类型的匹配
  */
object CaseStruct extends App{

  // 匹配数组
  val intArr = Array(1, 1, 3, 4, 5, 6)

  println("array match...")
  intArr match {
    case Array(x ,2, _*) =>
      println(x)
    case Array(a,b,c,d,e,f) =>
      println(a+b+c+d+e+f)
    case _ => println("Case nothing..")
  }

  // 匹配链表
  //val intList = List(1,2,3,4)
  val intList = List(1,2)
  val one = 1
  val two = 2
  val three = 3
  val four = 4
  println("List match...")

  /**
    * 注意：在Scala中列表要么为空（Nil表示空列表）要么是一个head元素加上一个tail列表。
    * 9 :: List(5, 2)  :: 操作符是将给定的头和尾创建一个新的列表
    *  注意：:: 操作符是右结合的，如9 :: 5 :: 2 :: Nil相当于 9 :: (5 :: (2 :: Nil))
    */
  intList match {
    case List(one,two,three) =>  // List(1,2,3)
      println(intList.sum)
    case one::two::three::four => // List(1,2,3,4)
      intList.foreach(println)
    case one::Nil  =>            // List(1)
      printf("only %d.", one)
    case one::tail  =>           // List(1..........)
      printf("%d.....", one)
    case _ => println("Case nothing..")
  }

  // 匹配元祖
  val tup = (2, 3, 7)
  tup match {
    case (1, x, y) => println(s"1, $x , $y")
    case (_, z, 5) => println(z)
    case  _ => println("else")
  }
}
