package cn.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GroupByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GroupByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val classScore = Array(
      ("Hadoop01", 80),
      ("Hadoop02", 83),
      ("Hadoop01", 85),
      ("Hadoop04", 80),
      ("Hadoop05", 87),
      ("Hadoop03", 80),
    )

    //转化集合为初始RDD
    val classSoreRdd: RDD[(String, Int)] = sc.parallelize(classScore)
    //对班级按成绩分组
    val classSoreRdd2: RDD[(String, Iterable[Int])] = classSoreRdd.groupByKey();
    classSoreRdd2.foreach(f => {
      val className = f._1
      val iterable: Iterator[Int] = f._2.iterator
      var total = 0
      while (iterable.hasNext) {
        total += iterable.next()
      }
      println(s"$className:$total")
    })

    //保存每一个班级的总成绩
    val className_totalSocre: RDD[(String, Int)] = classSoreRdd2.map(f=>{
      val iterator = f._2.iterator
      var total = 0
      while (iterator.hasNext) {
        total += iterator.next()
      }
      (f._1, total)
    })
  }
}
