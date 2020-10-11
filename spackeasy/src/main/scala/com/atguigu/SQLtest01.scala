package com.atguigu

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SQLtest01 {
  def main(args: Array[String]): Unit = {
    //创建配置文件
    val sparkConf: SparkConf = new SparkConf().
      setAppName("atguigu").setMaster("local[*]")

    //创建会话session
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //建立隐式函数
    import spark.implicits._

    val frame: DataFrame = spark.read.json("./user.json")

    frame.show()
    //dataframe==>rdd
    val rdd: RDD[Row] = frame.rdd


    //dataframe==>dataset
    val ds: Dataset[people] = frame.as[people]
    ds.createTempView("zxg")
     spark.sql("select * from zxg").show()




  }

}
case class people(username:String,age:BigInt)
