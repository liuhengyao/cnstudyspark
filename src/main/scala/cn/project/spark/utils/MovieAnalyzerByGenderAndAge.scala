package cn.project.spark.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.{SparkConf, SparkContext}

object MovieAnalyzerByGenderAndAge {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    var masterUrl = "local[2]"
    var dataPath = "" //数据存放路径
    //1.线上spark作业一般都动态指定模式与数据地址
    //第一个参数只传入spark集群的url,第二个参数传入的数据地址信息
    if(args.length > 0) {
      masterUrl = args(0)
    } else if(args.length > 1){
      dataPath = args(1)
    }
    val conf = new SparkConf().setAppName("MovieAnalyzerByGenderAndAge").setMaster(masterUrl)
    val sc: SparkContext = new SparkContext(conf)
    //spark2.0官网统一的Hive和SQL的入口
    val sparkSql = SparkSession.builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "D:\\hadoop\\data\\spark_files")
      .enableHiveSupport()
      .getOrCreate()

    //使用RDD读取数据
    val userRDD = SparkUtils.findUserRDD(sc)
    val movieRDD = SparkUtils.findMovieRDD(sc)
    val occupationRDD = SparkUtils.findOccupationRDD(sc)
    val ratingsRDD = SparkUtils.findRationsRDD(sc)

    /**
      * 功能：实现某部电影观看者中男性和女性不同年龄分别有多少人
      * 1.从点评数据中心获取观看者的信息ID
      * 2.把rating和User表进行join操作获得用户的性别信息
      * 3.使用内置的函数进行信息统计和分析
      */
    //定义表的schema模式
    val schemaforusers = new StructType("")
  }
}
