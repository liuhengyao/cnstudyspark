package cn.streaming.study.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountKafkaReceiver {
  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("WordCountKafkaReceiver").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))

    //配置zk地址
    //SparkStreaming 消费kafka数据，
    val zkQuorum: String = "node1:2181,node2:2181:node3:2181"
    //程序属于哪个消费组
    val groupId = "test-consumer-group"
    //程序消费的主题
    val topics = "xs_wordcount,xs_wordcount1"
    //配置kafka主题对应的消费线程数
    val threadNumForEachTopic = 2
    val topicsMap = topics.split(",").map((_, threadNumForEachTopic)).toMap
    //基于Receiver方式消费数据默认的存储级别
    //StorageLevel.MEMORY_AND_DISK_SER_2
    val topicLinesOffset: DStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topicsMap)
    //获取行的内容部分
    val topicLines: DStream[String] = topicLinesOffset.map(_._2)
    //词频统计
    val wordCount: DStream[(String, Int)] = topicLines.flatMap((_.split(" "))).map((_, 1)).reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
