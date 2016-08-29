package com.yiban.datacenter.Spark_MLlib

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    val s=new String("1,2.0,2.3")
    println( s.split(',')
        .map(x=>x.toDouble)
        .foreach { x => println(x) } )

  }

}
