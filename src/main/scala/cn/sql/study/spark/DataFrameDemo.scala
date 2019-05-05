package cn.sql.study.spark

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object DataFrameDemo {

  def main(args: Array[String]): Unit = {
    //spark1.x的版本入口
    val conf = new SparkConf().setAppName("DataFrameTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val stuDF: DataFrame = sqlContext.read.format("json").load("D:\\student.json")
    //hive 入口
    val hiveContext = new HiveContext(sc)

    //spark 2.x 入口
    val spark2SQl = SparkSession
      .builder()
      .appName("DataFrameTest")
      .master("local")
      //spark2.0之后，使用sparksession-hive必须设置仓库路径
      .config("spark.sql.warehouse.dir", "D:\\")
      .enableHiveSupport()
      .getOrCreate()
    //读取本地文件
    //spark2.0 DataFrame只是DataSet的一种类型
    //DataFrame = DataSet[Row]
    val stuDFS: DataFrame = spark2SQl.read.format("json").load("")
    val stuDS: Dataset[Row] = spark2SQl.read.json("")
//  stuDFS.printSchema()
//  stuDFS.show()
    //DataFrame注册为临时表
    stuDFS.registerTempTable("xds_01") //1.6之前
    stuDFS.createOrReplaceTempView("sx_stu01")
    val sxStu: Dataset[Row] = spark2SQl.sql("select * from sx_stu01 where age>18")
    //持久化
    sxStu.write.saveAsTable("sx_stu01")
  }
}
