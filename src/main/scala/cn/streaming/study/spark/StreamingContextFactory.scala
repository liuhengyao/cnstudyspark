package cn.streaming.study.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class StreamingContextFactory extends Function0 [StreamingContext]{
  override def apply(): StreamingContext = {
    val conf = new SparkConf().setMaster("local[4]").setAppName("UserVisitSessionAnalyzeSpark")
    conf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("hdfs://node1/sparkDirAd")
    ssc
  }
}
