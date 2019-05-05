package cn.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object JoinDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Join").setMaster("local")
    val sc = new SparkContext(conf)
    //测试数据生成
    val className = Array(
      ("1701", "麻子"),
      ("1702", "张三"),
      ("1703", "李四"),
    )
    val classSore = Array(
      ("1701", 90),
      ("1702", 80),
      ("1703", 60),
    )
    //集合转化RDD
    val nameRdd1 = sc.parallelize(className)
    val classRdd1 = sc.parallelize(classSore)
    val classNameRdd: RDD[(String, (String, Int))] = nameRdd1.join(classRdd1)
    val nameClassRdd = classNameRdd.map(f=>(f._2._2,f._2._1))
    val nameClassRddSort = nameClassRdd.sortByKey(false)
    nameClassRddSort.foreach(f=>println(s"$f._2:$f._1"))
  }
}
