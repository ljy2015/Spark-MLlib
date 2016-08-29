package com.welab.PPD.PPDdemo

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.AssociationRules
import scala.reflect.io.File
import java.io.PrintWriter

object FPDemo {
 
  def main(args: Array[String]): Unit = {
    
    val conf=new SparkConf().setAppName("FP-Growth").setMaster("local")
    val sc=new SparkContext(conf)
    
    val filename="E:\\MyFile\\Work\\Drug\\all_taobao_lists_new3"
    val masterdata = sc.textFile(filename)
    
    val transactions: RDD[Array[String]] = masterdata.map(s => s.trim.split(","))
    
    val fpg = new FPGrowth()
      .setMinSupport(0.1)
      .setNumPartitions(10)
    val model = fpg.run(transactions)

/*    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }*/

    val minConfidence = 0.6
    val result= model.generateAssociationRules(minConfidence).collect().map { rule =>
      
/*      println(
      rule.antecedent.mkString("[", ",", "]")
        + " => " + rule.consequent .mkString("[", ",", "]")
        + ", " + rule.confidence)*/
        rule.antecedent.mkString("[", ",", "]") + " => " + rule.consequent .mkString("[", ",", "]") + ", " + rule.confidence
      }
    printFile(result)
    //result.foreach { x => println(x) }
    
    }
  
  def printFile(strs:Array[String]){
    if(strs.isEmpty)
      return
    val outfile=new PrintWriter("taobao_result3.txt")
    for(str <- strs){
      outfile.println(str)
    }
    outfile.close()
    
  }
  
}