package cn.streaming.study.spark

import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * SparkStreaming高可用
  * 1.Driver高可用
  *
  */
object WordCountKafkaReceiverHA {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    //自定义上下文
    val checkpointDirectory = "hdfs://node1/sparkDirAd"
    //优化1：Driver高可用
    //优化2：启动预写日志、设置检查点（自定义类里面）
    //val conf = new SparkConf().setAppName("WordCountKafkaReceiver")
    val contextFactory = new StreamingContextFactory()
    val ssc = StreamingContext.getOrCreate(checkpointDirectory, contextFactory)
    //配置zk地址
    val zkQuorum: String = "node1:2181,node2:2181,node3:2181"
    //属于哪个消费组
    val groupId = "test-group"
    val topics = "it_wordcount,it_wordcount1"
    val topcisMap = topics.split(",").map((_, 2)).toMap
    //设置DStream持久化
    val topicsLines_offset1: DStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId, topcisMap, StorageLevel.MEMORY_AND_DISK_SER)
    val groupId2 = "test-group2"
    val topicsLines_offset2: DStream[(String, String)] = KafkaUtils.createStream(ssc, zkQuorum, groupId2, topcisMap, StorageLevel.MEMORY_AND_DISK_SER)
    //使用ssc.union()
    val topcisLines_offset: DStream[(String, String)] = topicsLines_offset1.union(topicsLines_offset2)
    //获取行的内容
    val topicLines: DStream[(String)] = topcisLines_offset.map(_._2)
    val wordCount: DStream[(String, Int)] = topicLines.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
    wordCount.print()
    ssc.start()
    ssc.awaitTermination()

  }
}
