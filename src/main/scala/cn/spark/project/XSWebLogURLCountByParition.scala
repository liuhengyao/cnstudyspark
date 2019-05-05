package cn.spark.project

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 统计每个子网站下top2 并按学院分区
  */
object XSWebLogURLCountByParition {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WebLogURLByPartition").setMaster("local")
    val sc = new SparkContext(conf)

    //读取文件
    val linesRdd: RDD[String] = sc.textFile("D:\\xsweb.log")
    //每个网页的个数(String, Int)
    val urlLogCountRdd: RDD[(String, Int)] = linesRdd.map(f=>{
      val urllist = f.split("\t")
      (urllist(1), 1)
    })
    //每个域名下，每个子网页个数(String, (String, Int))
    val urlHostCountRdd: RDD[(String, (String, Int))] = urlLogCountRdd.reduceByKey(_+_).map(f=>{
      val url = new URL(f._1)
      (url.getHost, f)
    })
    //获取总的学院数组
    val hostnameArr = urlHostCountRdd.map(_._1).distinct().collect()
    //按host分组聚合
    val urlHostGroupRdd: RDD[(String, Iterable[(String, Int)])] = urlHostCountRdd.partitionBy(new MyPartiontioner(hostnameArr)).groupByKey()
    //取每个子网页前2名
    val urlUrltop2Rdd: RDD[(String, List[(String, Int)])] = urlHostGroupRdd.map(f=>{
      val list: List[(String, Int)] = f._2.toList.sortBy(_._2).reverse.take(2)
      (f._1, list)
    })
    urlUrltop2Rdd.saveAsTextFile("D:\\out0001")
    sc.stop()
  }

  //自定义分区规则
  class MyPartiontioner(hostname: Array[String]) extends Partitioner {
    //构建map存储host和分区索引
    val hashMap = new mutable.HashMap[String, Int]()
    var index = 0
    for(host <- hostname) {
      hashMap.put(host, index)
      index += 1
    }

    override def numPartitions: Int = hostname.length

    override def getPartition(key: Any): Int = {
      val newKey = key.toString
      val partIndex = hashMap.get(newKey) match {
        case None => 0
        case Some(v) => v
      }
      partIndex
    }
  }
}
