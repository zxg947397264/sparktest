package com.atguigu.ddr

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test01_worldcount {
  def main(args: Array[String]): Unit = {

  // 创建Spark运行配置对象
  val sparkConf = new SparkConf().setMaster("local[*]").setAppName("test01_worldcount")

  // 创建Spark上下文环境对象（连接对象）
  val sc : SparkContext = new SparkContext(sparkConf)

  // 读取文件数据
  val fileRDD: RDD[String] = sc.textFile("./input/zxg")

  // 将文件中的数据进行分词
  val wordRDD: RDD[String] = fileRDD.map{
    a=>
      val strings: Array[String] = a.split(":")
      "hdfs debug recoverLease -path "+strings(0)+" -retries 20"
      }

  // 转换数据结构 word => (word, 1)
 // val word2OneRDD: RDD[(String, Int)] = wordRDD.map((_,1))

  // 将转换结构后的数据按照相同的单词进行分组聚合
 // val word2CountRDD: RDD[(String, Int)] = word2OneRDD.reduceByKey(_+_)

  // 将数据聚合结果采集到内存中
  //val word2Count: Array[(String, Int)] = word2CountRDD.collect()

  // 打印结果
    wordRDD.foreach(println)

  //关闭Spark连接
  sc.stop()
  }

}
