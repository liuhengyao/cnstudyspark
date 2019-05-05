package cn.spark.project

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * 需求2： 求出每一个用户停留时间最长的两个基站的x,y坐标
  */
object UserPhoneLocationCoo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserPhoneLocationCoo").setMaster("local")
    val sc = new SparkContext(conf)
    //获取用户基站信息rdd数据
    val userData = sc.textFile("D:\\hadoop\\data\\spark_files\\bs_log")

    //行内容-->((手机号,基站),时间)
    val ph_station_time: RDD[((String, String), Long)] = userData.map(f => {
      val splitsField: Array[String] = f.split(",")
      //手机号
      val phone = splitsField(0)
      //时间
      var times = (splitsField(1)).toLong
      //基站
      val station = splitsField(2)
      //状态
      val out_in = splitsField(3).toInt
      if (out_in == 1) {
        //进入基站 -times
        times = -times;
      }
      ((phone, station), times)
    })
    //(手机号，站点)，时间 --> (站点，（手机号，时间））
    val sitePhoneTimeRdd: RDD[(String, (String, Long))] = ph_station_time.reduceByKey(_+_).map(f=>(f._1._1,(f._1._2, f._2)))
      .groupByKey()
      .map(f => {
        var list = f._2.iterator.toList.sortBy(_._2).reverse
        if(list.size > 2) {
          list = list.take(2)
        }
        (f._1,list)
      })
      .flatMap(f => {
        val list2 = new ListBuffer[(String, (String, Long))]
        for(e <- f._2) {
          list2.append((e._1, (f._1, e._2)))
        }
        list2
      })
    //坐标消息
    val userXY = sc.textFile("D:\\loc_info")
    val stationXY: RDD[(String, (String, String))] = userXY.map(f=>{
      val field = f.split(",")
      (field(0), (field(1), field(2)))
    })
    //join联合
    val phoneStationXY: RDD[(String, ((String, Long), (String, String)))] = sitePhoneTimeRdd.join(stationXY)
    phoneStationXY.foreach(println)
  }
}
