package cn.project.spark.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
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
    //User.dat => Row(UserID,Gender,Age,Occupation,Zip-code)
    val schemaforusers = new StructType("UserID::Gender::Age::OccupationID::Zipcode".split("::")
    .map(column => StructField(column, StringType, true)))
    val userRDDRows: RDD[Row] = userRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim))
    val userDS: Dataset[Row] = sparkSql.createDataFrame(userRDDRows, schemaforusers)
    //Ratings.dat -> Row(UserID,MiveID,Ratings,timestamp)
    val schemaforatings = new StructType("UserID::MiveID::Ratings::timestamp".split("::")
    .map(column => StructField(column, DataTypes.StringType, true)))
    val ratingRDDRows: RDD[Row] = ratingsRDD.map(_.split("::")).map(line => Row(line(0).trim, line(1).trim, line(2).trim, line(3).trim))
    val ratingsDS: Dataset[Row] = sparkSql.createDataFrame(ratingRDDRows, schemaforatings)

    ratingsDS.createOrReplaceTempView("tb_rating")
    userDS.createOrReplaceTempView("tb_users")
    sparkSql.sql("select t.Gender,t.age,count(t.UserID) cnt from (" +
      "select r.UserID, r.MiveId, r.Rating, r.timstamp, u.Gender ,u.Age, u.Occupation" +
      "u.Zipcode from tb_rating r join tb_users u on r.UserID=u.UserID and r.MiveId == 1193) t " +
      "group by t.Gender,t.Age").show()
    ratingsDS.filter(s" MovieID = 1193")
      .join(userDS, "UserID")
      .select("Gender", "Age")
      .groupBy("Gender", "Age")
      .count()
      .show(10)
    sc.stop()
  }
}
