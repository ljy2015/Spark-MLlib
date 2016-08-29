package com.welab.taobao_drug

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors

object myKMeans {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("myKMeans").setMaster("local")
    val sc = new SparkContext(conf)
    // Load and parse the data
    val data = sc.textFile("E:\\MyFile\\Work\\taobao\\no_index_result_normalized.txt")
    val parsedData = data.map(s => Vectors.dense(s.split("\t").map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 4
    val numIterations = 1000
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    println("clustering center is :")
    clusters.clusterCenters.foreach { x => x.toArray.foreach{x=>print(x+",")}; println("********")}
    
    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    val testdata = sc.textFile("E:\\MyFile\\Work\\taobao\\result_normalized.txt").map { x => 
      val fields=x.split("\t")  
      val account=fields(0)
      val a=fields.slice(1, fields.length)
      (account,Vectors.dense(a.map { x => x.toDouble }))
    }
    
    val result=clusters.predict(testdata.map{x=> x._2})
    
    //result.saveAsTextFile("E:\\MyFile\\Work\\taobao\\result")

  }
}