package com.welab.PPD.PPDdemo

import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
//import org.apache.spark.mllib.fpm.AssociationRules

object DataPreproccess {
    def printset(myset:Set[String]):String={
    var str=""
    for(s <- myset){
      if(s==myset.head)
        str=str+s;
      else
        str=str+","+s
    }
    str
  }
  
  
  def main(args: Array[String]): Unit = {
    val conf=new SparkConf().setAppName("FP-Growth-DataPreproccess").setMaster("local")
    val sc=new SparkContext(conf)
    val logInfodata = sc.textFile(
        "file:\\C:\\Users\\Administrator\\Desktop\\data\\PPD-First-Round-Data-Updated\\PPD-First-Round-Data-Update\\TrainingSet\\LogInfo.csv")

    val logInfo = logInfodata.map(s => s.trim.split(",")).map{x=> (x(0),x(2))}
    //println(logInfo.first)
    val masterdata=sc.textFile("file:\\C:\\Users\\Administrator\\Desktop\\data\\PPD-First-Round-Data-Updated\\PPD-First-Round-Data-Update\\TrainingSet\\Master.csv")
    
    val master = masterdata.map(s => s.trim.split(",")).map(x=>(x(0),x(x.length-2)))
    //println(master.first())
    
    val data=logInfo.join(master).map{ x=> 
        (x._1,x._2._1+","+x._2._2)
    }
    
    println(data.first())
    
    val result0=data.filter { x => 
      val words=x._2.split(",")
      words(words.length-1).trim().toInt==0
    } //去掉类别
    .map{x=>
      val words=x._2.split(",")
      (x._1,words(0))
    }//合并同一个用户的行为
    .reduceByKey{ (x,y) =>
      x+","+y  
    }.map{x=>
      val nums=x._2.split(",")
      printset(nums.toSet)
    }
    
    println(result0.first())
   result0.saveAsTextFile("file:\\C:\\Users\\Administrator\\Desktop\\data\\PPD-First-Round-Data-Updated\\PPD-First-Round-Data-Update\\TrainingSet\\LogInforesult-0")
    
   val result1=data.filter { x => 
      val words=x._2.split(",")
      words(words.length-1).trim().toInt==1
    }//去掉类别
    .map{x=>
      val words=x._2.split(",")
      (x._1,words(0))
    } //合并同一个用户的行为
    .reduceByKey{ (x,y) =>
      x+","+y  
    }.map{x=>
      val nums=x._2.split(",")
      printset(nums.toSet)
    }
    println(result1.first())
   
    result1.saveAsTextFile("file:\\C:\\Users\\Administrator\\Desktop\\data\\PPD-First-Round-Data-Updated\\PPD-First-Round-Data-Update\\TrainingSet\\LogInforesult-1")
    
  }
}