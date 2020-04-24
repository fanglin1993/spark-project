package com.scz.test

import scala.collection.mutable

/**
  * Created by shen on 2020/2/26.
  */
object MyTest {
  def main(args: Array[String]): Unit = {
    val hourCountMap = new mutable.HashMap[String, Long]
    val key = hourCountMap.get("key")
    println(key)
    println(key == null)
    println(key == None)
    println("-----------------")
    val hourCountMap2 = new java.util.HashMap[String, Long]
    val key2 = hourCountMap2.get("key")
    println(key2)
    println(key2 == null)
    println(key2 == None)
    println(2.compareTo(3))
  }
}
