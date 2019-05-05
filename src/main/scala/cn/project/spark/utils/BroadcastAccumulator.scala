package cn.project.spark.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object BroadcastAccumulator {

  def main(args: Array[String]): Unit = {

  }

  /**
    * Spark提供的Broadcast Variable,是只读的。并且在每个节点的Excutor上只会有一份副本
    * 而不会为每个task拷贝一份副本。因此其最大的作用，就是减少变量到各个节点的网络传输消耗
    * 以及在各个节点的内存消耗。此外，spark自己内部也使用了高效的广播算法来减少网络消耗
    * *
    * 可以通过调用SparkContext的broadcast()方法，来针对某个变量创建广播变量。
    * 然后在算子的函数内，使用广播变量时，每个Excutor只会拷贝一个副本。每个节点可以使用广播变量的
    * value()方法获取值。记住，广播变量，是只读的。
    */
  def broadCastValue() = {
    val conf = new SparkConf().setAppName("UserPhoneLocationTime").setMaster("local")
    val sc = new SparkContext(conf)

    //算子内使用外部变量例子
    val arr = Array(1, 2, 3, 4, 5)
    val arrRdd = sc.parallelize(arr)
    //要求被每个rdd值*2
    val value = 2
    val rdd1 = arrRdd.map(a =>
      //算子内使用外部变量，变量value将会被发送到每一个task上
      a * value
    )
    //优化方式：使用广播参数将参数发送到Excutor上
    val broadcast = sc.broadcast(value)
    val rdd2 = arrRdd.map(f => {
    val vae = broadcast.value
    f * vae
    })
  }

  /**
    * (2) Accumulator
    * Spark提供的Accumulator,主要用于多个节点对一个变量进行共享性的操作
    * Accumulator只提供了累加功能，但是却给我们提供了多个task对一个变量并行操作的功能
    * 但是task只能对Accumulator进行累加操作，不能读取它的值
    * 只有Driver程序可以读取Accumulator的值
    */
  def accumulatorTest() = {
    val conf = new SparkConf().setAppName("UserPhoneLocationTime").setMaster("local")
    val sc = new SparkContext(conf)

    val arr = Array(1, 3, 5, 7, 9, 10)
    val rdd1:RDD[Int] = sc.parallelize(arr)
    //Driver客户端定义累加器
    val accumulator = sc.longAccumulator
    var sum = 0
    //累计所有分区内的值
    rdd1.foreach(f => {
      accumulator.add(f)
    })
    //在Driver客户端读取
    println(accumulator.value)
  }
}
