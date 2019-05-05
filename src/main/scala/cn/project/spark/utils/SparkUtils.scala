package cn.project.spark.utils

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object SparkUtils {

    //参与用户
    def findUserRDD(sc: SparkContext): RDD[String] = {
      sc.textFile("D:\\User.data.parquet")
    }

    //影片信息
    def findMovieRDD(sc: SparkContext): RDD[String] = {
      sc.textFile("D:\\Movies.dat.parquet")
    }

    //职业
    def findOccupationRDD(sc: SparkContext): RDD[String] = {
      sc.textFile("D:\\Occupations.dat.parquet")
    }

    //等级评分（评分）
    def findRationsRDD(sc: SparkContext): RDD[String] = {
      sc.textFile("D:\\Ratings.dat.parquet")
    }
}
