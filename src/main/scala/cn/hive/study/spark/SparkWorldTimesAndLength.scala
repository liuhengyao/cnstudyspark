package cn.hive.study.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 统计每个单词的长度和单词的个数
  */
object SparkWorldTimesAndLength {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("WordTimesLength").setMaster("local")
    val sc: SparkContext = new SparkContext(conf)
    val spark2Sql = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir", "D:\\spark_files\\warehouse")
      .enableHiveSupport()
      .getOrCreate()

    val linesRdd: RDD[String] = sc.textFile("D:\\words.txt")
    val wordsRdd: RDD[String] = linesRdd.flatMap(_.split(" "))
    val wordClassRdd: RDD[Word] = wordsRdd.map(Word(_))

    //使用sql函数：统计每个单词的长度和每个单词的次数
    import spark2Sql.implicits._
    val wordDF: Dataset[Word] = wordClassRdd.toDS()
    wordDF.createOrReplaceTempView("tb_word")
    spark2Sql.sql("select count(wd),length(wd) from tb_word group by wd").show()
    //自定义udf聚合函数：统一每个单词长度和每个单词次数
    spark2Sql.udf.register("mylength", (wd: String)=>wd.length)
    spark2Sql.udf.register("myudfcount", new MyUDFCount())
    spark2Sql.sql("select mylength(wd), myudfcount(wd) from tb_word group by wd").show()
  }
}

case class Word(wd: String)

//自定义UDF函数分组统计效果
class MyUDFCount22() extends UserDefinedAggregateFunction {
  //UDF接收参数的数据类型
  override def inputSchema: StructType = StructType(List(
    StructField("word", DataTypes.StringType, true)
  ))

  //数据在聚合计算过程中的值得数据类型
  override def bufferSchema: StructType = StructType(
    List(
      StructField("word1", DataTypes.StringType, true)
    )
  )

  //返回数据类型为分组内单词出现的次数
  override def dataType: DataType = DataTypes.StringType

  //传入的参数类型与返回的数据类型是否一致
  override def deterministic: Boolean = true

  //初始化参数的初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = " "

  //再次接受参数的时候，聚合函数的计算规则，传入一个单词+1
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getString(0)+"-"+input.getString(0)
  }

  //合并缓存计算结果
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getString(0)+"-"+buffer2.getString(0)
  }

  //UDAF计算的结果返回
  override def evaluate(buffer: Row): Any = buffer.getString(0)
}

class MyUDFCount() extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(
    List(
      StructField("word", DataTypes.StringType, true)
    )
  )

  override def bufferSchema: StructType = StructType(
    List(
      StructField("count", DataTypes.IntegerType, true)
    )
  )

  override def dataType: DataType = DataTypes.IntegerType

  override def deterministic: Boolean = false

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getInt(0) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
  }

  override def evaluate(buffer: Row): Any = buffer.getInt(0)
}

