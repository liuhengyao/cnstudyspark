package cn.project.spark.utils

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 豆瓣影院数据分析平台
  * 用户行为分析：用户观看电影和点评电影的所有行为分析
  * 采集：将server中的数据发给kafka(具备实时性)
  * 过滤：将server端进行数据过滤和格式化（sparksql过滤）
  * 处理：
  * 1）一个基本的技巧就是先去传统的sql实现数据的业务逻辑（模拟数据）
  * 2）使用DataSet或DataFrame去实现业务的功能（统计分析功能）
  * 3）RDD实现业务的功能，运行的时候是基于RDD的
  *
  * 数据：格式为Parquet
  * 1."ratings.bat": UserID::MiveID::Rationg::timestamp
  * 2."User.dat": UserID::Gender::Age::OccupationID::Zip-code(邮编)
  * 3."Movies.dat": MovieID::Title::Genres(类型)
  * 4."Occupation.dat": OccupationID::OccupationName 数据量小我们要用的，hashSet数据结构为了mapJoin
  */
object DouBanMovieUserAnalyzer {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]" //程序运行在本地的local中，用于测试
    var dataPath = "" //数据的存放路径
    //第一个参数只传入spark集群的URL,第二个参数传入的是数据的地址信息
    if(args.length > 0) {
      masterUrl = args(0)
    } else if (args.length > 1) {
      dataPath = args(1)
    }
    val conf = new SparkConf().setMaster(masterUrl).setAppName("DouBanMovieUserAnalyzer")
    val sc = new SparkContext(conf)

    //使用RDD读取数据
    val userRDD = SparkUtils.findUserRDD(sc)
    val movieRDD = SparkUtils.findMovieRDD(sc)
    val occupationRDD = SparkUtils.findOccupationRDD(sc)
    val ratingRDD = SparkUtils.findRationsRDD(sc)

    //模块1
    analyzeMovie(userRDD, occupationRDD, ratingRDD)
    //模块2
    sc.stop()
  }

  //豆瓣电影用户分析行为：分析具体某部电影观看的用户信息，例如电影ID为1193
  def analyzeMovie(userRDD: RDD[String], occupationRDD: RDD[String], ratingsRDD: RDD[String]): Unit = {
    //用户职业关联
    val userBasic = userRDD.map(_.split("::")).map(user => (user(3), (user(0), user(1), user(2))))
    //(Occupation, OccupationName)
    val occupationBasic = occupationRDD.map(_.split("::")).map(job => (job(0), job(1)))
    //(OccupationID, ((UserID, Gender, Age), OccupationName))
    val occupaUser = userBasic.join(occupationBasic)
    //过滤电影id=1193的
    val ratingsUser = ratingsRDD.map(_.split("::")).map(rat => (rat(0), rat(1))).filter(_._2.equals("1192"))
    //(UserID, ((UserID, Gender, Age), OccupationName))
    val userInfomation = occupaUser.map(x => (x._2._1._1, x._2))
    //(UserID, (MovieID, ((UserID,Gender,Age), occupationName)))
    val userInfoForSpecificMovie = ratingsUser.join(userInfomation)
    val userSecificMovie = userInfoForSpecificMovie.collect()
    for(ele <- userSecificMovie){
      println(ele)
    }
  }

  /**
    * 豆瓣电影流行度分析，所有电影中平均得分最高（口碑最好）的电影及观看人数最高的电影（流行度最高）
    * “ratings.dat”:UserID::MovieID::Rating::timestamp
    *
    * @param userRDD
    * @param ratingsRDD
    */
  def analyzerPolularMovie(sc: SparkContext, userRDD: RDD[String], ratingsRDD: RDD[String]) = {
    //cache()和checkpoint的讲解
    //格式化出电影的id和评分（UserID, MiveID, Rating）
    val movieRating = ratingsRDD.map(_.split("::")).map((f => (f(0), f(1), f(2)))).cache()
    //一般使用checkpoint之前必须先调用setCheckPointDir方法
    sc.setCheckpointDir("hdfs://node1:8020/sparkdir")
    //并且被checkpoint算子，最好先调用cache算子缓存数据，避免重新开启一个单独的job再次计算
    movieRating.checkpoint()
    //格式化key-value (MovieID, (Rating, 1))
    val ratingReduce = movieRating.map(rat => (rat._2, (rat._3.toInt, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    ratingReduce
      //.map(f => (f._2._1, f._1))
      .map(x => (x._2._1.toDouble / x._2._2, x._1))
      .sortByKey(false)
      .take(1)
      .foreach(println)

    //分析粉丝或观看人数最多的电影（基于电影数据要进行cache）
    val moviePopular = movieRating
      .map(mr => (mr._2, 1))
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .take(1)
      .foreach(println)
  }

  /**
    * 计算分析出最受不同年龄人员喜欢的电影top10
    *"UserID.dat":UserID::Gender::Age::OccupationID::Zip-code(邮编)
    * 思路：还是计算TopN,但是这里关注亮点
    * 1.不同年年龄阶段如何界定，关于这个是业务的问题不是技术问题，当然你在实际实现的时候可以用RDD的filter
    * 例如13< age <18 这样做会导致运行大量的计算，因为要进行扫描，所以非常消耗性能
    * 所以一般情况下，我们都是在原始数据中直接对要进行分组的年龄段提前做好ETL,例如进行ETL产生的数据
    * 1： “Under 18”
    * 18: "18-24"
    * 25: "25-34"
    * 35: "35-44"
    * 45: "45-49"
    * 50: "50-55"
    * 56: "56+"
    * 2.性能问题
    * 1）因为要扫描，所以非常消耗性能，我们通过提前的ETL计算发生在以前，用空间换时间，
    * 当然这些也可以用hive做，因为Hive语法非常的强悍并且内置了很多函数
    * 2）在这里要使用mapjoin,原因是targeUsers数据只有Age,数据量一般不会太多
    */

  def analyzerPopularMovieAge(sc: SparkContext, userRDD: RDD[String], movieRDD: RDD[String], ratingRDD :RDD[String]): Unit = {
    //(UserID, Age)
    //if (f._2.toInt >= 18 && f._2.toInt <= 20) true else false
    val target18Users = userRDD.map(_.split("::")).map(f => (f(0), f(3))).filter(age => age._2.equals("18"))
    val target25Users = userRDD.map(_.split("::")).map(f => (f(0), f(3))).filter(age => age._2.equals("25"))
    //Spark中mapjoin的实现思路
    //借助于Broadcast, 会把数据广播到Executor级别让所有的任务共享该唯一的数据
    //而不是每次运行task的时候都要发送一份数据的拷贝，这显著降低了网络数据传输和JVM内存的消耗
    //(UserID,Age)->UserID

    //性能优化：下面的操作本来可以通过distinct()来实现对算子的算术去重，但了为了
    // 避免shuffle过程，所有先使用collect收集数据（包含重复数据）并且发送给客户端
    //在客户端使用hashset对象实现对元素去重
    val target18UsersSet = mutable.HashSet() ++ target18Users.map(_._1).collect()
    val target25UsersSet = mutable.HashSet() ++ target25Users.map(_._1).collect()
    //发送广播
    val traget18UsersBroadcast = sc.broadcast(target18UsersSet)
    val traget25UsersBroadcast = sc.broadcast(target25UsersSet)

    //所有电影18岁用户最喜爱电影TopN分析
    //Ratings.dat:UserID::MovieID::Rating::timestamp

    //(MoiveID, Title)
    val movieIDName = movieRDD
      .map(_.split("::"))
      .map(f=>(f(0), f(1)))
      .collect()
      .toMap
    //(UserID, MoiveID)
    ratingRDD.map(_.split("::"))
      .map(f=> (f(0), f(1)))
      .filter(x =>
        //判断用户是否属于目标年龄用户（x._1=UserID）
        traget18UsersBroadcast.value.contains(x._1)
      )
      .map(x => (x._2, 1))
      .reduceByKey(_+_)
      .sortBy(_._2, false)
      .take(10)
      .map(x => {
        (movieIDName.getOrElse(x._1, null), x._2)
      })
      .foreach(println)

    //25岁最喜爱电影topN分析
    ratingRDD.map(_.split("::")).map(x => (x(0), x(1))).filter(x =>
      traget25UsersBroadcast.value.contains(x._1)
    ).map(x => (x._2, 1)).reduceByKey(_+_)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .take(10)
      .map(x => (movieIDName.getOrElse(x._2, null), x._1))
      .foreach(println)

  }

}
