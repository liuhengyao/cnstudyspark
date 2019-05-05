package cn.streaming.study.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 基于HDFS SparkStreaming 监听HDFS目录
  * 1.提交SparkStreaming 作业
  * /home/hadoop/spark-2.2.0-hadoop/bin/spark-submit \
  * --class cn.xsit.spark.demo.WordCountHDFS \
  * /home/hadoop/cn.xsit.spark-1.0-SNAPSHOT.jar
  *
  * 2.往监听目录输入流
  * hdfs fs -put ./fff /xsstreamingdir/t1
  */
object WordCountHDFS {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("CountHDFS").setMaster("local[2]")
    val stream = new StreamingContext(conf, Seconds(10))
    //监听hdfs系统目录
    val lines: DStream[String] = stream.textFileStream("/xsstreamingdir")
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordOne: DStream[(String, Int)] = words.map((_, 1))
    val wordCount: DStream[(String, Int)] = wordOne.reduceByKey(_+_)
    wordCount.print()
    stream.start()
    stream.awaitTermination()
  }
}
