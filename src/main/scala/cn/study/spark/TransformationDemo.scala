package cn.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformationDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Transformation").setMaster("local")
    val sc = new SparkContext(conf)
    //建立集合数据源
    val arr = Array(1, 2, 3, 4, 5)
    val rdd1: RDD[Int] = sc.parallelize(arr, 2)
    val rdd2: RDD[Int] = rdd1.map(_ * 5)
    val result1 = rdd2.collect()
    val result2 = rdd1.filter(_ % 2 == 0).collect()
    for( e<- rdd1) println(e)
  }
}
