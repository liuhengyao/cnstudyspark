package cn.hive.study.spark

import org.apache.spark.sql._

/**
  * 基于Spark-on-hive操作
  */
object SparkHiveDemo {

  def main(args: Array[String]): Unit = {
    val spark2Sql = SparkSession
      .builder()
      .appName("SparkHiveDemo")
      .master("local")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    //基于hive操作
    //使用hive语句创建表，加载数据，分析表
    spark2Sql.sql("DROP DATABASE IF EXISTS xsaccess cascade")
    spark2Sql.sql("CREATE DATABASE IF NOT EXISTS xsaccess")
    spark2Sql.sql("CREATE TABLE IF NOST EXISTS xsaccess.tb_xslog(xsdate STRING,xstimestamp STRING,xsuserid STRING,pageid STRING, section STRING,action STRING)")
    spark2Sql.sql("load data local inpath '/home/hadoop/data/access.log' overwrite into table tb_xslog")
    val dateCnt: Dataset[Row] = spark2Sql.sql("select xsdate,count(*) as cnt from xsaccess.tb_xslog group by xsdate")
    dateCnt.show()
    //持久化到hive，永久存储
    dateCnt.write.saveAsTable("xsaccess.usercnt_date")
    spark2Sql.stop()
  }
}
