package com.yiban.datacenter.Spark_MLlib

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.stat.{ MultivariateStatisticalSummary, Statistics }
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Statistis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("yarn-cluster")
    val sc = new SparkContext(conf)

    val observations = sc.textFile("/user/liujiyu/spark/mldata1.txt")
      .map(_.split(' ') //     转换为RDD[Array[String]]类型
        .map(_.toDouble)) //            转换为RDD[Array[Double]]类型
      .map(line => Vectors.dense(line)) //转换为RDD[Vector]类型

    // Compute column summary statistics.
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println(summary.mean) // a dense vector containing the mean value for each column
    println(summary.variance) // column-wise variance
    println(summary.numNonzeros) // number of nonzeros in each column

    import org.apache.spark.SparkContext
    import org.apache.spark.mllib.linalg._
    import org.apache.spark.mllib.stat.Statistics

    val data1 = Array(1.0, 2.0, 3.0, 4.0, 5.0)
    val data2 = Array(1.0, 2.0, 3.0, 4.0, 5.0)
    val distData1: RDD[Double] = sc.parallelize(data1)
    val distData2: RDD[Double] = sc.parallelize(data2) // must have the same number of partitions and cardinality as seriesX

    // compute the correlation using Pearson's method. Enter "spearman" for Spearman's method. If a 
    // method is not specified, Pearson's method will be used by default. 
    val correlation: Double = Statistics.corr(distData1, distData2, "pearson")

    val data: RDD[Vector] = observations // note that each Vector is a row and not a column

    // calculate the correlation matrix using Pearson's method. Use "spearman" for Spearman's method.
    // If a method is not specified, Pearson's method will be used by default. 
    val correlMatrix: Matrix = Statistics.corr(data, "pearson")

  }
}