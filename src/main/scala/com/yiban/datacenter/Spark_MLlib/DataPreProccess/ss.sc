package com.yiban.datacenter.DataPreProccess

import scala.collection.mutable.LinkedList

object ss {

  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  var result: scala.collection.mutable.LinkedList[String] = scala.collection.mutable.LinkedList.empty
                                                  //> result  : scala.collection.mutable.LinkedList[String] = LinkedList()

  result.append(LinkedList[String]("ad"))         //> res0: scala.collection.mutable.LinkedList[String] = LinkedList(ad)
  println(result)                                 //> LinkedList()
  
  val s="1,2"                                     //> s  : String = 1,2
  val w=s.split(",")                              //> w  : Array[String] = Array(1, 2)
  println(w(0).toDouble)                          //> 1.0
}