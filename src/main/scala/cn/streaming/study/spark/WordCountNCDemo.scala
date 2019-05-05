package cn.streaming.study.spark

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 基于nc工具，sparkstreaming 监听端口，准时进行词频统计
  */
object WordCountNCDemo {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    //有两种创建StreamingContext方法
    /*val conf = new SparkConf().setAppName("WordCountNC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val stream = new StreamingContext(sc, Seconds(2))*/
    val conf = new SparkConf().setAppName("WordCountNC").setMaster("local[2]")
    //每隔2秒去处理一个数据段内的数据，这个数据段数据被封装为一个RDD
    val stream = new StreamingContext(conf, Seconds(2))
    //默认的容错级别：StorageLevel.MEMORY_AND_DISK_SER_2
    val lines: DStream[String] = stream.socketTextStream("node1", 8888)
    //获取行内容，做词频统计
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordOne: DStream[(String, Int)] = words.map((_, 1))
    val wordCount: DStream[(String, Int)] = wordOne.reduceByKey(_+_)
    wordCount.print() //“action”提交，必须的
    //让StreamingContext 处于运行状态
    stream.start()//stream启动之后就能在添加任何业务逻辑了
    stream.awaitTermination()
  }
}
