package cn.project.spark.utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DouBanMovieAnalyzerTwo {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    var masterUrl = "local[2]"
    var dataPath = "数据存放的路径"
    if(args.length > 0){
      masterUrl = args(0)
    }else if (args.length > 1){
      dataPath = args(1)
    }
    val conf = new SparkConf().setAppName("DouBanMovieUserAnalyzer").setMaster(masterUrl)
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerizlizer")
    conf.registerKryoClasses(Array(classOf[SecondarySortKey], classOf[String]))

    val sc: SparkContext = new SparkContext(conf)
  }

  /**
    * 计算分析出所有豆瓣电影中最受男性或女性喜爱的电影Top10
    *UserID::Gender::Age::OccupationID::Zip-code(邮编)
    * UserID::MiveID::Rating::timestamp
    * 分析：只从ratings中无法计算出最受男性和女性喜爱的电影Top10, 因为RDD中没有Gender
    * 如果我们需要使用Gender信息进行Gender的分类，就一定需要聚合，当然我们聚合使用的是mapjoin
    *(分布式数据的killer是数据倾斜，map端的join不会数据倾斜，用户一般很均匀的分布
    * （注意）：
    * 1.因为再次调用数据RDD,复用了之前cache的rating的数据
    * 2.在根据性别Gender过滤出数据之后关于TopN的部分复用前面的的代码就可以了
    * 3.ratings数据为（UserID,MiveID,Rating）,要进行join的话要变为Key-Value(UserId,(UserID,MiveID,Rating))
    * 4.进行join的时候通过take方法注意join后的数据格式
    * 5.使用数据冗余来实现代码复用和更高效的运行。这是企业级项目中非常重要的技巧
    * */
    def analyerPopularBySex(userRDD: RDD[String], ratingRDD: RDD[String]) = {
      val ratings = ratingRDD.map(_.split("::")).map(f => (f(0), f(1), f(2))).cache()
      println("所有电影中最受男性喜爱的电影Top10:")
      val male = "M"
      val female = "F"
      //join后的数据结果（UserID,((UserID,MiveID,Rating),Gender)）
      val genderRating = ratings.map(f => (f._1, (f._1, f._2, f._3))).join(
        userRDD.map(_.split("::")).map(f => (f(0), f(1)))
      ).cache()
      //过滤只有男性，女性的电影
      val maleRating = genderRating.filter(_._2._2.equals(male)).map(f => f._2._1)
      val femaleRating = genderRating.filter(_._2._2.equals(female)).map(f => f._2._1)
      maleRating.map(x => (x._2, (x._3.toInt, 1)))
        .reduceByKey((x, y) => (x._1+y._1, x._2+y._2))
        .map(f => (f._2._1.toDouble / f._2._2, f._1))
        .sortByKey(false)
        .map(x => (x._2, x._1))
        .take(10)
        .foreach(println)
      println("所有电影中最受女性喜爱的电影Top10:")
      femaleRating.map(x => (x._2, (x._3.toInt, 1)))
        .reduceByKey((x, y) => {
          (x._1 + y._1, x._2 + y._2)
        })
        .map(x => (x._2._1.toDouble / x._2._2, x._1))
        .sortByKey(false)
        .map(x => (x._2, x._1))
        .take(10)
        .foreach(println)
    }

  //最近点评信息分析
  def analyzerMovieRating(ratingRDD: RDD[String]) = {
    //对电影评分进行二次排序，以timestamp和rating两个维度降序排列
    //Rating.dat":UserID::MiveID::Rating::timestamp
    println("对电影评分数据进行二次排序，以timestamp和rating两个维度降序：")
    val pairWithSortKey = ratingRDD.map(line => {
      val splited = line.split("::")
      (new SecondarySortKey(splited(3).toDouble, splited(2).toDouble), line)
    })
    val sorted = pairWithSortKey.sortByKey(false)
    val sortedResult = sorted.map(sortLines => (sortLines._2))
    sortedResult.take(10).foreach(println)
  }
}
