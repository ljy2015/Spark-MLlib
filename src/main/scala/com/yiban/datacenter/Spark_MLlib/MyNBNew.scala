package com.yiban.datacenter.Spark_MLlib

import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.PrintWriter
import org.apache.spark.rdd.RDD

object MyNBNew {

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

    //var myTotalIndex: Array[Array[Double]] = Array[Array[Double]]()

    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    var myTotalIndex: RDD[Array[Double]] = sc.parallelize(Seq())

 /*   val trainpaths = Array(
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/IncludeHospital/group-1-include-hospital-matrix.txt",
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/IncludeHospital/group-2-include-hospital-matrix.txt",
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/IncludeHospital/group-3-include-hospital-matrix.txt",
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/IncludeHospital/group-4-include-hospital-matrix.txt",
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/IncludeHospital/group-5-include-hospital-matrix.txt");*/

    val trainpaths = Array(
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/UnIncludeHospital/group-1-matrix.txt",
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/UnIncludeHospital/group-2-matrix.txt",
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/UnIncludeHospital/group-3-matrix.txt",
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/UnIncludeHospital/group-4-matrix.txt",
      "hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/SecondData/UnIncludeHospital/group-5-matrix.txt");

    for (path <- trainpaths) {
      //println(path)
      var traindata: RDD[String] = sc.textFile("hdfs://192.168.27.233:8020/user/liujiyu/spark/FinalData/test/hello")
      for (trainpath <- trainpaths) {
        if (!trainpath.equals(path)) {
          //println("train is"+trainpath)
          traindata ++= sc.textFile(trainpath)
        }
      }

      val testdata = sc.textFile(path)

      val trainData1 = traindata.map { line =>
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

      val testData1 = testdata.map { line =>
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

      val model = NaiveBayes.train(trainData1, lambda = 1) //(training, lambda = 1.0, modelType = "multinomial")

      val results = testData1.map(p => (model.predict(p.features), p.label))
      myTotalIndex ++= computeAccuracy(results, testData1, sc)
      //computeAccuracy(results, testData1,sc).
    }

    /*    // Save and load model
    model.save(sc, "myModelPath")
    val sameModel = NaiveBayesModel.load(sc, "myModelPath")*/
    //computeFinalAccuracy(myTotalIndex)

    var AVG_Accuracy = sc.accumulator(0.0, "AVG_Accuracy")
    var AVG_P = sc.accumulator(0.0, "AVG_P")
    var AVG_R = sc.accumulator(0.0, "AVG_R")
    var AVG_F1 = sc.accumulator(0.0, "AVG_F1")

    if (myTotalIndex.count() != 5)
      println("myTotalIndex.count()!=5")

    myTotalIndex.foreach { x =>

      AVG_Accuracy += x(0)
      AVG_P += x(1)
      AVG_R += x(2)
      AVG_F1 += x(3)
    }

    println("AVG_Accuracy is " + AVG_Accuracy.value / 5)
    println("AVG_P is " + AVG_P.value / 5)
    println("AVG_R is " + AVG_R.value / 5)
    println("AVG_F1 is " + AVG_F1.value / 5)

    //myTotalIndex.map { x => println("accuracy is " + x(0) + "\t" + "P is " + x(1) + "\t" + "R is " + x(2) + "\t" + "F1 is " + x(3) + "\t") }.collect()
    val saveresult = myTotalIndex.map(x => (x(0), x(1), x(2), x(3)))
    saveresult.saveAsTextFile("/user/liujiyu/spark/FinalData/SecondData/IncludeHospital/output-uninclude-hospital")
    //saveresult.saveAsTextFile("/user/liujiyu/spark/FinalData/SecondData/IncludeHospital/output")
    sc.stop()
  }
  /**
   * 计算指标的过程
   */
  def computeAccuracy(results: RDD[(Double, Double)], test: RDD[LabeledPoint], sc: SparkContext): RDD[Array[Double]] = {
    //var TotalIndex = Array[Array[Double]]()

    val finalaccuracy = 1.0 * results.filter(x => x._1 == x._2).count() / test.count()
    //    println("accuracy is:" + finalaccuracy)

    //预测值为0，原值为0
    var P = 0.0 //精确率 ：阳性预测正确的个数 / 实际为阳性的总数
    var R = 0.0 //召回率 ：阳性预测正确的个数 / 预测为阳性的总数
    var a22 = 0.0
    var PSUM = 0.0
    var RSUM = 0.0
    for (i <- 0 to 2) { //预测值
      for (j <- 0 to 2) { //实际值
        val accuracys = 1.0 * results.filter(x => (x._1 == i && x._2 == j)).count().toDouble / test.count()
        if (i == 2) {
          if (j == 2) {
            a22 = accuracys
          }
          RSUM += accuracys
        }
        if (j == 2) {
          PSUM += accuracys
        }
        // print("accuracys is :" + accuracys + "\t")
      }
      //println()
    }

    P = a22 / PSUM
    R = a22 / RSUM
    val F1 = 2 * P * R / (P + R)

    //    println("P= "+P)
    //    println("R= "+R)
    //    println("F1= "+F1)

    //TotalIndex = TotalIndex.+:(Array[Double](finalaccuracy, P, R, F1))

    //val data = Array(finalaccuracy, P, R, F1)
    sc.parallelize(Seq(Array(finalaccuracy, P, R, F1)))
  }

  def computeFinalAccuracy(TotalIndex: Array[Array[Double]]) {
    if (TotalIndex.length != 5)
      println("TotalIndex.length!=5")
    var AVG_Accuracy: Double = 0.0;
    var AVG_P: Double = 0.0;
    var AVG_R: Double = 0.0;
    var AVG_F1: Double = 0.0;

    TotalIndex.foreach { x =>
      AVG_Accuracy += x(0)
      AVG_P += x(1)
      AVG_R += x(2)
      AVG_F1 += x(3)
      println("accuracy is " + x(0) + "\t" + "P is " + x(1) + "\t" + "R is " + x(2) + "\t" + "F1 is " + x(3) + "\t")
    }
    AVG_Accuracy = AVG_Accuracy / TotalIndex.length
    AVG_P = AVG_P / TotalIndex.length
    AVG_R = AVG_R / TotalIndex.length
    AVG_F1 = AVG_F1 / TotalIndex.length

    println("AVG_Accuracy is " + AVG_Accuracy)
    println("AVG_P is " + AVG_P)
    println("AVG_R is " + AVG_R)
    println("AVG_F1 is " + AVG_F1)
  }
}