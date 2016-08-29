package com.yiban.datacenter.Spark_MLlib

import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.mutable.Seq
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.BlockMatrix

object DataType {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("yarn-cluster")
    val sc = new SparkContext(conf)

    //********Local vector***********/
    val dv: Vector = Vectors.dense(1.0, 0.0, 3.0)

    //********Labeled point***********/
    val pos = LabeledPoint(1.0, Vectors.dense(1.0, 0.0, 3.0))
    //Local matrix
    val dm: Matrix = Matrices.dense(3, 2, Array(1.0, 3.0, 5.0, 2.0, 4.0, 6.0))
    val sm: Matrix = Matrices.sparse(3, 2, Array(0, 1, 3), Array(0, 2, 1), Array(9, 6, 8))
    //Distributed matrix

    //********RowMatrix***********/
    val rows = sc.textFile("/user/liujiyu/spark/mldata1.txt")
      .map(_.split(' ') //     转换为RDD[Array[String]]类型
        .map(_.toDouble)) //            转换为RDD[Array[Double]]类型
      .map(line => Vectors.dense(line)) //转换为RDD[Vector]类型

    // Create a RowMatrix from an RDD[Vector].
    val mat: RowMatrix = new RowMatrix(rows)

    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()
    println(m + "： " + n)
    // QR decomposition
    // val qrResult = mat.tallSkinnyQR(true)//1.5.0的新特性

    //********IndexedRowMatrix***********/
    val rows1 = sc.textFile("/user/liujiyu/spark/mldata1.txt")
      .map(_.split(' ') //     转换为RDD[Array[String]]类型
        .map(_.toDouble)) //            转换为RDD[Array[Double]]类型
      .map(line => Vectors.dense(line)) //转换为RDD[Vector]类型
      .map((vc) => new IndexedRow(vc.size, vc)) //IndexedRow 带有行索引的矩阵，初始化的参数，列数和每一行的vector

    val irm = new IndexedRowMatrix(rows1)

    //********CoordinateMatrix***********/
    /**
     *    这是分布式矩阵的第三种matrix.坐标矩阵也是一种RDD存储的分布式矩阵。
    顾名思义，这里的每一项都是一个(i: Long, j: Long, value: Double)
    指示行列值的元组tuple。 其中i是行坐标，j是列坐标，value是值。如果
    矩阵是非常大的而且稀疏，坐标矩阵一定是最好的选择。坐标矩阵则是通过
    RDD[MatrixEntry]实例创建，MatrixEntry是(long,long.Double)形式。
    坐标矩阵可以转化为IndexedRowMatrix。 
     * 矩阵文件为Latest5.txt：
                 1 1 4
                  2 6 2
                  1 3 4
                  2 3 4
                  2 8 1
                  3 2 4
                  5 1 3
                  5 4 7
                  5 7 2
                  7 2 3
                  7 3 2
                  7 4 3
                  6 2 3
                  6 4 3
                  6 8 1
                  8 2 4
                  8 5 8
                  8 8 10
     */
    val rows2 = sc.textFile("/user/liujiyu/spark/mldata1.txt")
      .map(_.split(' ') //     转换为RDD[Array[String]]类型
        .map(_.toDouble)) //            转换为RDD[Array[Double]]类型
      .map(m => (m(0).toLong, m(1).toLong, m(2)))
      .map((vc) => new MatrixEntry(vc._1, vc._2, vc._3)) //IndexedRow 带有行索引的矩阵，初始化的参数，列数和每一行的vector

    val cm = new CoordinateMatrix(rows2)

    
    //********BlockMatrix***********/
    //A BlockMatrix can be most easily created from an IndexedRowMatrix or CoordinateMatrix by calling toBlockMatrix.
    
    val matA: BlockMatrix = cm.toBlockMatrix().cache()

    // Validate whether the BlockMatrix is set up properly. Throws an Exception when it is not valid.
    // Nothing happens if it is valid.
    matA.validate()

    // Calculate A^T A.
    val ata = matA.transpose.multiply(matA)

    sc.stop();
  }
}