package cn.sql.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object DataSetDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataSetTest").setMaster("local")
    val sc = new SparkContext(conf)
    //spark2.x sql和hive入口
    val spark2Sql = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "D:\\spark2_warehouse")
      .enableHiveSupport()
      .getOrCreate()
    //使用sc读取本地文件
    val stuRdd: RDD[String] = sc.textFile("D:\\student.txt")
    val stuClassRdd: RDD[XSStudent] = stuRdd.map(f => {
      val str: Array[String] = f.split(",")
      XSStudent(str(0).toInt, str(1), str(2).toInt)
    })
    //将rdd转化为dataset并注册为临时表
    //需要隐式导入
    import spark2Sql.implicits._
    val stuDF: DataFrame = stuClassRdd.toDF()
    val stuDs: Dataset[XSStudent] = stuClassRdd.toDS()
    stuDs.show()
    //注册临时表
    stuDs.createOrReplaceTempView("xss_stu01")
    val cnstu: DataFrame = spark2Sql.sql("select * from xss_stu01")
    cnstu.show()
    cnstu.write.json("")
    cnstu.write.save("")
    spark2Sql.stop()
  }
}

case class XSStudent(id: Int, name: String, age: Int)
