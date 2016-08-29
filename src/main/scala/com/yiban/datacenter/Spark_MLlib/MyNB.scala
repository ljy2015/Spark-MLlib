package com.yiban.datacenter.Spark_MLlib

import org.apache.spark.mllib.classification.{ NaiveBayes, NaiveBayesModel }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import java.io.PrintWriter
import java.nio.file.Files
import java.io.File

object MyNB {

  def fileReading(path: String, outputpath: String) = {
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

      row.trim().foreach {
        x =>
          pw.append(s"data: $x").write("\n")
      }
      //keys ++= values(values.size - 1).split(" ").toSet
    }
    pw.flush
    pw.close

  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    val data1 = sc.textFile(args(0))

    /*var keys: scala.collection.mutable.Set[String] = scala.collection.mutable.Set.empty

    data1.map { line =>
      val values = line.split(',')
      keys ++= values(values.size - 1).split(" ").toSet
    }
    println("keys:" + keys)

    var row: String = ""
    val parsedData1 = data1.map { line =>

      row = ""
      val values = line.split(',')
      row += values(0) + ","
      for (word <- keys) {
        if (!values(values.size - 1).split(" ").contains(word)) {
          row += "0 "
        } else
          row += "1 "
      }
      println("row:" + row)
      val s = row.trim().split(",")
      LabeledPoint(s(0).toDouble, Vectors.dense(s(s.size - 1).split(",").map(_.toDouble)))
    }*/

    //var (flag, result) = fileReading("/home/liujiyu/spark/NB/log-file.20160119-11.log", sc)
    // var (flag, result) = fileReading("E:\\liujiyu\\DtaPreProcess\\log-file.20160119-112.log")

    //    val data = sc.parallelize(result, 1)
    //    val parsedData = data.map { line =>
    //      //println(line)
    //      val parts = line.split(",")
    //      if (parts(0).contains('1')) {
    //        LabeledPoint("1".toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    //      } else if (parts(0).contains("0")) {
    //        LabeledPoint("0".toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    //      } else {
    //        LabeledPoint("2".toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    //      }
    //    }
    //parsedData.foreach { x => println(x.label+":"+x.features.toString()) }

    //        parsedData1.foreach { x => print(x.label)
    //          print(":")
    //          x.features.toArray.map(x=>print(x+"")) 
    //          println()}

    // Split data into training (60%) and test (40%).
    val parsedData1 = data1.map { line =>
      val parts = line.split(',')
      if (parts(0).contains('1')) {
        LabeledPoint("1".toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      } else if (parts(0).contains("0")) {
        LabeledPoint("0".toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      } else {
        LabeledPoint("2".toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
      }
      //LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }
    parsedData1.saveAsTextFile(args(0)+File.separator+"output")
    println("total sum:" + parsedData1.count())

    val splits = parsedData1.randomSplit(Array(0.2, 0.8), seed = 11L)
    val training = splits(0)
    val test = splits(1)

    val model = NaiveBayes.train(training, lambda = 1) //(training, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = test.map(p => (model.predict(p.features), p.label))

    println("test count is :" + test.count())

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / test.count()
    println("accuracy is:" + accuracy)

    //预测值为0，原值为0
    var P=0.0  //精确率 ：阳性预测正确的个数 / 实际为阳性的总数
    var R=0.0  //召回率 ：阳性预测正确的个数 / 预测为阳性的总数
    var a22=0.0
    var PSUM=0.0
    var RSUM=0.0
    for (i <- 0 to 2) {//预测值
      for (j <- 0 to 2) {//实际值
        val accuracys = 1.0 * predictionAndLabel.filter(x => (x._1 == i && x._2 == j)).count() / test.count()
        if(i==2){
          if(j==2){
            a22=accuracys
          }
          RSUM+=accuracys
        }
        if(j==2){
          PSUM+=accuracys
        }
        print("accuracys is :" + accuracys +"\t")
      }
      println()
    }
    
    P=a22/PSUM
    R=a22/RSUM
    val F1=2*P*R/(P+R)
    
    println("P= "+P)
    println("R= "+R)
    println("F1= "+F1)

    /*    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = NaiveBayesModel.load(sc, "myModelPath")*/

    sc.stop()
  }

}