package cn.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object XSWordCount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("XSWordCount")
    val sc: SparkContext = new SparkContext(conf)
    //读取数据
    sc.textFile(args(0))
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_+_)
      .saveAsTextFile(args(1))
    sc.stop()

    //集合数据转化为RDD类型
    val list: RDD[Int] = sc.parallelize(List(1,2,3,5,6,7,9,10))

  }
}
