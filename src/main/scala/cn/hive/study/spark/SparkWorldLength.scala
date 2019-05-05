package cn.hive.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计每个单词的长度
  */
object SparkWorldLength {
  val conf = new SparkConf().setAppName("WordCountLength").setMaster("local")
  val sc = new SparkContext(conf)
  val spark2Sql = SparkSession
    .builder()
    .appName("worldcount")
    .config(conf)
    .getOrCreate()
  val linesRdd: RDD[String] = sc.textFile("D:\\word.txt")
  val wordsRdd: RDD[String] = linesRdd.flatMap(_.split(" "))
  val wordRdd: RDD[Word] = wordsRdd.map(Word(_))
  //（string,length)
  val wordLength: RDD[(String, Int)] = wordsRdd.map(f => (f, f.length))
  wordLength.foreach(println)
  //使用内置的函数获取单词的长度
  import spark2Sql.implicits._
  val wordDs: Dataset[Word] = wordRdd.toDS()
  wordDs.createOrReplaceTempView("t_word001")
  spark2Sql.sql("select wd,length(wd) from t_word001").show()
  //自定义UDF函数获取单词长度
 /* val foo = udf(() => Math.random())
  spark.udf.register("random", foo.asNondeterministic())*/
  spark2Sql.udf.register('mylength', (wd: String) => wd.length)
  spark2Sql.sql("select mylength(wd) from tb_word").show()
  spark2Sql.stop()

}

case class Word(wd: String)
