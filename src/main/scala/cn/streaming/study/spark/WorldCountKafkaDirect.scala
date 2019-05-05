package cn.streaming.study.spark

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 首先启动zk
  * bin/kafka-server-start.sh config/server.properties
  * 创建topic
  * bin/kafka-topics.sh --list zookeeper node1:2181
  * 启动一个生产者发送消息
  * bin/kafka-console-producer.sh --broker-list node1:9092 --topic t_worldcount
  * 启动spark-streaming应用程序
  */
object WorldCountKafkaDirect {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("WorldCountKafka").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(2))
    //sparkstreaming 消费主题
    val topics = "t_wordcount"
    //配置kafka主题对应的消费线程数
    val topicsMap = topics.split(",").toSet[String]
    //基于Receiver方式消费数据默认的存储级别
    StorageLevel.MEMORY_AND_DISK_SER_2
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "node1:9092")
    val linesDS: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsMap)
    //获取行的内容部分
    val topicLines: DStream[String] = linesDS.map(_._2)
    //词频统计
    val wordCount: DStream[(String, Int)] = topicLines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    //output操作
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
