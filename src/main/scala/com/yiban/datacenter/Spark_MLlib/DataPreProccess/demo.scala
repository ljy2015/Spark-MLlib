package com.yiban.datacenter.DataPreProccess

import java.nio.file.Path
import java.io.File
import java.nio.file.Files
import scala.io.Codec
import java.security.Key
import java.util.TreeMap.KeySet
import java.util.TreeMap.KeySet
import java.util.List
import scala.collection.mutable.LinkedList
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import java.io.PrintWriter
import scala.collection.mutable.ArrayBuffer

object demo {

   def fileReading(path: String): (Boolean, scala.collection.mutable.ListBuffer[String]) = {
    if (path == null) {
      println("input path is error!")
      return (false, scala.collection.mutable.ListBuffer[String]())
    }
    var flag = false;
    var result: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer.empty
    import java.io.File
    import scala.io.Source

    val infile = new File(path)
    
    
    if (infile.isDirectory()) {
      val files = infile.listFiles()
      for (file <- files) {
        //用于求出包含所有的不重复的关键字
        val lineIterator_sum = Source.fromFile(file, "UTF-8").getLines()

        /**
         * 统计所有的不重复的关键字，并排序
         */
        var keys: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty

        for (line <- lineIterator_sum) {
          val values = line.split(",")
          keys ++= values(values.size - 1).split(" ").toSet
        }
        //println(keys.size) // l is a String
        //println(keys)
        /**
         * 形成最后算法所要的矩阵result,每一行用字符串的形式保存
         */
        val lineIterator_matrix = Source.fromFile(file, "UTF-8").getLines()

        var row: String = ""
        for (line <- lineIterator_matrix) {
          row = ""
          val values = line.split(",")
          row += values(0) + ","
          for (word <- keys) {

            if (!values(values.size - 1).split(" ").contains(word)) {
              row += "0 "
            } else
              row += "1 "
          }

          result.append(row.trim())

          //keys ++= values(values.size - 1).split(" ").toSet
        }
        // println(result)
      }
      flag = true;
    } else {
      println("input a file, you should input a directory! ")
    }

    (flag, result)
  }
   
   def fileRead(path: String, outputpath: String) = {
    if (path == null) {
      println("input path is error!")

    }
    var flag = false;
    var result: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer.empty
    import java.io.File
    import scala.io.Source

    val infile = new File(path)
    //    if (infile.isDirectory()) {
    //      val files = infile.listFiles()
    //      for (file <- files) {
    //用于求出包含所有的不重复的关键字
    val lineIterator_sum = Source.fromFile(infile, "UTF-8").getLines()

    val pw = new PrintWriter(new File(outputpath))

    /**
     * 统计所有的不重复的关键字，并排序
     */
    var keys: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty

    for (line <- lineIterator_sum) {
      val values = line.split(",")
      keys ++= values(values.size - 1).split(" ").toSet
    }
    //println(keys.size) // l is a String
    //println(keys)
    /**
     * 形成最后算法所要的矩阵result,每一行用字符串的形式保存
     */
    val lineIterator_matrix = Source.fromFile(infile, "UTF-8").getLines()
    //val lineIterator_matrix = sc.textFile("/home/liujiyu/spark/NB/log-file.20160119-112.log")

    var row: String = ""
    for (line <- lineIterator_matrix) {
      row = ""
      val values = line.split(",")
      row += values(0) + ","
      for (word <- keys) {

        if (!values(values.size - 1).split(" ").contains(word)) {
          row += "0 "
        } else
          row += "1 "
      }

      pw.append(row.trim())
      pw.append("\n")
     
      //keys ++= values(values.size - 1).split(" ").toSet
    }
    
    pw.flush
    pw.close

  }

   def featurenum(path: String) = {
    if (path == null) {
      println("input path is error!")

    }
    var flag = false;
    var result: scala.collection.mutable.ListBuffer[String] = scala.collection.mutable.ListBuffer.empty
    import java.io.File
    import scala.io.Source

    val infile = new File(path)
    //    if (infile.isDirectory()) {
    //      val files = infile.listFiles()
    //      for (file <- files) {
    //用于求出包含所有的不重复的关键字
    val lineIterator_sum = Source.fromFile(infile, "UTF-8").getLines()

    
    /**
     * 统计所有的不重复的关键字，并排序
     */
    var keys: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty

    for (line <- lineIterator_sum) {
      val values = line.split(",")
      keys ++= values(values.size - 1).split(" ").toSet
    }
    
    println(keys.size)

  }
   

  def main(args: Array[String]): Unit = {

    //fileRead("E:\\liujiyu\\DtaPreProcess\\log-file.20160119-17.log","E:\\liujiyu\\DtaPreProcess\\log-file.20160119-17.log.txt")
    
    //featurenum("E:\\liujiyu\\DtaPreProcess\\log-file.20160119-11.log")
    
    for(a<- '0' to '2'){
      println(a-48)
    }
    var P = ArrayBuffer[Double](0.0, 0.0, 0.0)

    
  }
}