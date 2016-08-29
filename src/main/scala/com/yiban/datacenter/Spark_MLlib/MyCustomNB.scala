package com.yiban.datacenter.Spark_MLlib

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.io.File

class element {
  var occur = 0
  var classnum = 0
}

object MyCustomNB {

  def train(path: String): (scala.collection.mutable.HashMap[(String, Char), Int], Int, Int, Int) = {
    if (path == null) {
      println("input path is error!")

    }
    var flag = false;
    var result: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer.empty
    import java.io.File
    import scala.io.Source
    var keys: scala.collection.mutable.HashMap[(String, Char), Int] = new scala.collection.mutable.HashMap[(String, Char), Int]()
    val infile = new File(path)
    //    if (infile.isDirectory()) {
    //      val files = infile.listFiles()
    //      for (file <- files) {
    //用于求出包含所有的不重复的关键字

    val lineIterator_sum = Source.fromFile(infile, "UTF-8").getLines()
    //println("train samples is :"+lineIterator_sum.length)

    /**
     * 统计所有的关键字的个数
     */
    var ZeroNUM = 0
    var OneNUM = 0
    var TwoNUM = 0

    for (line <- lineIterator_sum) {
      val values = line.split(",")
      //println(values(0).trim().last)
      values(0).trim().last match {
        case '0' => ZeroNUM += 1
        case '1' => OneNUM += 1
        case _   => TwoNUM += 1
      }

      val allkey = values(values.size - 1).split(" ").toSet;
      for (value <- allkey) {

        if (keys.contains((value, values(0).trim().last))) {

          var occur = keys.getOrElse((value, values(0).trim().last), 0)

          keys.update((value, values(0).trim().last), occur + 1)

        } else
          keys.put((value, values(0).trim().last), 1)
      }

      //      keys ++= values(values.size - 1).split(" ").toSet
    }
    // keys.foreach(x => println(x._1 + ":" + x._2))
    //println(keys.size)

    (keys, ZeroNUM, OneNUM, TwoNUM)

  }

  def predict(model: (HashMap[(String, Char), Int], Int, Int, Int), keys: Array[String], Thrmax: Double,Thrmin: Double): Int = {
     if (keys == null)
      return 0
    var P = ArrayBuffer[Double](-1.0, -1.0, -1.0)
    //println(P.size)

    //keys.foreach { x => println(x) }

    for (key <- keys) {
      //println("key is "+key)
      for (classnum <- '0' to '2') {
        //println("(key,classnum) is "+(key,classnum))
        val fenzi = model._1.getOrElse((key, classnum), 0)
        
        //println("fenzi is :"+fenzi)
        // println(classnum+":"+model.productElement(classnum.toInt-47))
        val fenmu = model.productElement(classnum.toInt - 47).asInstanceOf[Number].intValue()

        
        if (fenmu != 0 ) {
          //println(fenzi/fenmu)
          P(classnum.toInt - 48) match {
            case -1.0 => P(classnum.toInt - 48) = (fenzi.toDouble / fenmu)
            case _   => P(classnum.toInt - 48) *= (fenzi.toDouble / fenmu)
          }
       
        } else { //这种情况是出现了新词，新词暂时不考虑

        }

        //P.productElement(classnum.toInt-47).asInstanceOf[Number].doubleValue()*(fenzi/fenmu )
      }
    }
    for(i<- 0 to P.size-1){
      if(P(i)<0){
        P(i)=0
      }
    }
     
    
    var trainSUM = 0
    for (i <- 0 to 2) {
      trainSUM += model.productElement(i + 1).asInstanceOf[Number].intValue()
    }
    for (i <- 0 to P.size - 1) {
  
      P(i) = P(i) * (model.productElement(i + 1).asInstanceOf[Number].intValue().toDouble / trainSUM)
      
    }
    val Psum=P.sum
    for (i <- 0 to P.size - 1) {

      P(i) = P(i) /Psum
      
    }
    //P(i)=P(i)/Psum
    // P.foreach { x => println(x) }

    import scala.math._
    var maxindex = 0
    // for (elem <- 0 to P.length - 1) {

    if (P(2) >= Thrmax)
      maxindex = 2
    else if(P(0)<Thrmin)
      maxindex = 0
    else
      maxindex=1
    //}
    maxindex

  }

  def predict(testpath: String, model: (HashMap[(String, Char), Int], Int, Int, Int), Thrmax: Double,Thrmin: Double): (ArrayBuffer[String], ArrayBuffer[String]) = {
    val lineIterator_sum = Source.fromFile(new File(testpath), "UTF-8").getLines()
    var shijiLabel = new ArrayBuffer[String]()
    var testLabel = new ArrayBuffer[String]()
    for (line <- lineIterator_sum) {
      val values = line.split(",")
      shijiLabel += values(0)
      testLabel += predict(model, values(values.size - 1).split(" "), Thrmax,Thrmin).toString()
    }
    println("test samples is :" + testLabel.length)
    if (shijiLabel.length != testLabel.length) {
      println("shijiLabel.length!=testLabel.length")
    }
    (shijiLabel, testLabel)
  }

  def main(args: Array[String]): Unit = {

    var model = train("E:\\liujiyu\\DtaPreProcess\\train.log")

    println("model dict is :" + model._1)
    println("model dict size is :" + model._1.size)
    println("train classnum is:" + model._2 + ":" + model._3 + ":" + model._4)

    var (shijiresult, testresult) = predict("E:\\liujiyu\\DtaPreProcess\\test.log", model, 0.8,0.2)

    //预测值为0，原值为0
    var P = 0.0 //精确率 ：阳性预测正确的个数 / 实际为阳性的总数
    var R = 0.0 //召回率 ：阳性预测正确的个数 / 预测为阳性的总数

    var PSUM = 0.0
    var RSUM = 0.0
    var accuracynum = 0;
    var a00 = 0;
    var a01 = 0;
    var a02 = 0;
    var a10 = 0;
    var a11 = 0;
    var a12 = 0;
    var a20 = 0;
    var a21 = 0;
    var a22 = 0;
    println(testresult.length)
    println(shijiresult.length)
    for (i <- 0 to testresult.length - 1) { //预测值
      //        println(testresult(i).toCharArray().length)
      //        println((testresult(i), shijiresult(j)).toString())

      //        if(testresult(i).contains("2")&&shijiresult(j).contains("2")){
      //          println("sucess")
      //        }
      if (testresult(i).contains("0") && shijiresult(i).contains("0")) {
        a00 += 1;
      } else if (testresult(i).contains("0") && shijiresult(i).contains("1")) {
        a01 += 1;
      } else if (testresult(i).contains("0") && shijiresult(i).contains("2")) {
        a02 += 1;
      } else if (testresult(i).contains("1") && shijiresult(i).contains("0")) {
        a10 += 1;
      } else if (testresult(i).contains("1") && shijiresult(i).contains("1")) {
        a11 += 1;
      } else if (testresult(i).contains("1") && shijiresult(i).contains("2")) {
        a12 += 1;
      } else if (testresult(i).contains("2") && shijiresult(i).contains("0")) {
        a20 += 1;
      } else if (testresult(i).contains("2") && shijiresult(i).contains("1")) {
        a21 += 1;
      } else {
        a22 += 1;
      }

    }

    val accuracy: Double = (a00 + a11 + a22).toDouble / testresult.size
    println("accuracy is :" + accuracy)

    println(a00 + ":" + a01 + ":" + a02)
    println(a10 + ":" + a11 + ":" + a12)
    println(a20 + ":" + a21 + ":" + a22)

    PSUM = (a02 + a12 + a22).toDouble / testresult.size
    RSUM = (a20 + a21 + a22).toDouble / testresult.size
    P = a22.toDouble / (testresult.size * PSUM)
    R = a22.toDouble / (RSUM * testresult.size)
    val F1 = 2 * P * R / (P + R)

    println("P= " + P)
    println("R= " + R)
    println("F1= " + F1)

  }

}