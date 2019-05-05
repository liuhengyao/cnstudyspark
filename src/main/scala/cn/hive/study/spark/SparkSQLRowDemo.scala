package cn.hive.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQLRowDemo {
  val conf = new SparkConf().setAppName("RowDemo").setMaster("local")
  val sc = new SparkContext(conf)
  val spark2Sql = SparkSession
    .builder()
    .appName("test")
    .config(conf)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate()
  //使用sc读取本地普通文件
  val productRdd: RDD[String] = sc.textFile("D:\\sales.txt")
  //使用样例类转化为新的rdd
  val productRowRdd: RDD[Product] = productRdd.map(f => {
    val p: Array[String] = f.split(",")
    Product(p(0), p(1), p(2).toInt)
  })
  import spark2Sql.implicits._
  val proDs: Dataset[Product] = productRowRdd.toDS()
  //转化为临时表
  proDs.createOrReplaceTempView("product")
  val proTop3: DataFrame = spark2Sql.sql("select t.pname,t.pcategory,t.revenue from(select p.pname, p.category, p.revenue, " +
    "row_number() over(partintion by p.pcategory order by p.revenue desc) rownum from product p) t where t.rownum<=3 ")
  proTop3.show()
  spark2Sql.stop()
}

case class Product(pname: String, pcategory: String, revenue: Int)
