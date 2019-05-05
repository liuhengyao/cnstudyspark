package cn.streaming.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * DStream的transform算子：可以用于执行任意的RDD到RDD的转换操作。它可以用于实现
  */
object FilterBlackName {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setMaster("local[2]").setAppName("SparkStreamingNC")
    val sc = new SparkContext(conf)
    val streamContext = new StreamingContext(conf, Seconds(2))

    //黑名单测试数据（模拟黑名单库）
    val blkNamelist = Array(("laowang", true), ("xiaowang", true))
    val blkListRdd: RDD[(String, Boolean)] = sc.parallelize(blkNamelist)

    //用户日志规则：
    //nc -lk 8888
    //xiaowang,nan,18
    val lines: DStream[String] = streamContext.socketTextStream("node1", 8888)
    val studentsDs: DStream[Students] = lines.map(f=>(Students(f.split(",")(0), f.split(",")(1), f.split(",")(2).toInt)))
    val userList: DStream[Students] = studentsDs.transform(stuRdd => {
      val nameStu: RDD[(String, Students)] = stuRdd.map(s=>(s.name, s))
      val username: RDD[(String, (Students, Option[Boolean]))] = nameStu.leftOuterJoin(blkListRdd)
      //白名单
      username.filter(f => !f._2._2.getOrElse(false))
      val stuListName = username.map(f => f._2._1)
      stuListName
    })

    userList.print()
    streamContext.start()
    streamContext.awaitTermination()
  }
}

case class Students(name: String, sex: String, age: Int)