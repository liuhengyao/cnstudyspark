package cn.project.spark.utils

import kafka.serializer.StringDecoder
import org.apache.commons.lang.time.FastDateFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 实时分析观看某部电影的用户
  * 思路：
  * 1.获取Kafka的数据，将数据切分
  * 2.过滤需要的数据（MiveID）
  * 3.按时间窗口进行分组
  * 4.观看电影所用的平均时间间隔
  * 5.判断是否是观看该电影的用户
  * 6.将数据存储到redis的数据库
  */
object MoviePlayingAnalyzer {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("MoviePlayingAnalyzer").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(sparkConf)
    //产生数据批次的时间间隔为10s
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(10))
    //基于Direct方式消费kafka数据
    val dateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val topics = "tp_moovielog"
    val topicMap = topics.split(",").toSet[String]
    val kafkaParam: Map[String, String] = Map("metadata.broker.list"->"node:9092")
    val dStream: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParam, topicMap)
    //取出数据，把key扔掉
    val lines: DStream[String] = dStream.map(_._2)
    //切分数据
    val filterLines = lines.map(_.split("::")).filter(f => {
      f(1) == "1193"
    })
    val userAndTime: DStream[(String, Long)] = filterLines.map(f=> (f(2), f(3).toLong))
    //按时间窗口进行分组
    //每隔20秒统计30秒内的数据
    val groupWindow: DStream[(String, Iterable[Long])] = userAndTime.groupByKeyAndWindow(Seconds(30), Seconds(20))
    //为了避免误判，获取的数据大于等于0才做分析
    val filtered: DStream[(String, Iterable[Long])] = groupWindow.filter(_._2.size > 0)
    //得到每次观看电影所用的平均时间间隔
    val itemAvgTime: DStream[(String, Long)] = filtered.mapValues(it => {
      val list = it.toList.sorted
      val size = list.size
      val first = list(0)
      val last = list(size-1)
      val diff = last - first
      diff / size
    })
    //最终判断为观看该电影的用户
    val watchUser: DStream[(String, Long)] = itemAvgTime.filter(_._2 < 10000)
    watchUser.foreachRDD(rdd => {
      rdd.foreachPartition(it => {
        val connection = JedisConnectionPool.getConnetion()
        it.foreach(t => {
          val user = t._1
          val avgTime = t._2
          val currentTime = System.currentTimeMillis()
          connection.set(user+"_"+currentTime, avgTime.toString())
        })
        connection.close()
      })
    })
    watchUser.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
