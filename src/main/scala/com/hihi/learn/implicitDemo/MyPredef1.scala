package com.hihi.learn.implicitDemo

object MyPredef1 {
  implicit def manToSuperMan(man:Man) = new SuperMan(man)
}
