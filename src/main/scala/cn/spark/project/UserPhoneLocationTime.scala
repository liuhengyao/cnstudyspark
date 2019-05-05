package cn.spark.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 需求1：求每个人在基站的停留时间
  * 需求2：求出每一个人停留时间最长的两个基站
  */
object UserPhoneLocationTime {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserPhoneLocationTime").setMaster("local")
    val sc = new SparkContext(conf)
    //需求1
    stayTime(sc)
    //需求2
    siteRank(sc)
  }

  def stayTime(sc: SparkContext) = {
    //获取用户基站信息rdd
    val userData = sc.textFile("D:\\bs_log")
    //((phone, site), time)
    val phStationTime: RDD[((String, String), Long)] = userData.map(f=>{
      val splitField: Array[String] = f.split(",")
      val phone: String = splitField(0)
      var time: Long = splitField(1).toLong
      val site: String = splitField(2)
      val status: Int = splitField(3).toInt
      if(status == 1) {
        time = -time
      }
      ((phone, site), time)
    })
    //每个人在每个基站的时间
    val sitePhoneTimes: RDD[((String, String), Long)] = phStationTime.reduceByKey(_+_)
    val phStationSubit = sitePhoneTimes.foreach(println)
  }

  def siteRank(sc: SparkContext) = {
    //获取用户基站信息rdd
    val userData = sc.textFile("D:\\bs_log")
    //((phone, site), time)
    val phStationTime: RDD[((String, String), Long)] = userData.map(f=>{
      val splitField: Array[String] = f.split(",")
      val phone: String = splitField(0)
      var time: Long = splitField(1).toLong
      val site: String = splitField(2)
      val status: Int = splitField(3).toInt
      if(status == 1) {
        time = -time
      }
      ((phone, site), time)
    })
    //每个人在每个基站的时间
    val sitePhoneTimes: RDD[((String, String), Long)] = phStationTime.reduceByKey(_+_)
    //每个人下面的基站和时间
    val userSiteTime: RDD[(String, (String, Long))] = sitePhoneTimes.map(f=>(f._1._1, (f._1._2, f._2)))
    val userGroupTime: RDD[(String, Iterable[(String, Long)])] = userSiteTime.groupByKey()
    //取时间最长的两个基站
    val userTimeTop2: RDD[(String, List[(String, Long)])] = userGroupTime.map(f=>{
      val list: List[(String, Long)] = f._2.toList.sortBy(_._2).reverse.take(2)
      (f._1, list)
    })
    userTimeTop2.foreach(f=> println(f))
  }

}
