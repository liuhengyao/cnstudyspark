package cn.spark.project

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 每个网站下子网页的Top2
  */
object XSWebLogURLCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WebURLCount").setMaster("local")
    val sc = new SparkContext(conf)

    //读取本地的测试数据
    val linesLogRdd: RDD[String] = sc.textFile("D:\\xsweb.log")
    //(url,1)
    val logURLOneRdd:RDD[(String, Int)] = linesLogRdd.map(f=>{
      val log = f.split("\t")
      (log(1),1)
    })
    val logUrlCount:RDD[(String, Int)] = logURLOneRdd.reduceByKey(_+_)
    //(学院域名，（url,点击次数)
    val logHostUrlCount:RDD[(String, (String, Int))] = logUrlCount.map(f=>{
      val url = f._1
      val host = new URL(url)
      (host.getHost, f)
    })
    //将host分为同一组，在组内获取点击量为前2的值
    val hostGroupURLCount:RDD[(String, Iterable[(String, Int)])] = logHostUrlCount.groupByKey()
    //获取每个域名下点击量最多的两个子网页
    val hostTopList:RDD[(String, List[(String, Int)])] = hostGroupURLCount.map(f=>{
      val host = f._1
      val topList: List[(String, Int)] = f._2.toList.sortBy(_._2).reverse.take(2)
      (host, topList)
    })
    //提交作业
    hostTopList.foreach(f=>{
      val host = f._1
      val top2 = f._2
      for(e <- top2)
        println(s"$host,${e._1},${e._2}")
    })
    sc.stop()
  }
}
