package com.atguigu.ddr

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test06 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test02_ddr")
    val sc = new SparkContext(conf)
    val vlaue: RDD[String] = sc.textFile("./user_visit_action.txt",1)
    val unit: RDD[(String, String)] = vlaue.map {
      a =>
        val strings: Array[String] = a.split("_")
        (strings(2), strings(3))
    }

      val value: RDD[(String, String)] = unit.groupByKey().map(a=>a._2.toList).flatMap(a=>a.sliding(2).toList).filter(a=>a.length>1).map(a=>(a(0),a(1)))
      val value1: RDD[((String, String), Int)] = value.map(a=>((a),1)).reduceByKey(_+_)
      val value2: RDD[(String, (String, Int))] = value1.map(a=>(a._1._1,(a._1._2,a._2)))

      val idsum: RDD[(String, Int)] = unit.map(a=>(a._2,1)).reduceByKey(_+_)

    val value3: RDD[(String, (String, Double))] = value2.leftOuterJoin(idsum).mapValues(a=>(a._1._1,a._1._2/a._2.get.toDouble))
    val value4: RDD[String] = value3.map(a=>(a._1+"-"+a._2._1+"转换率为"+a._2._2))
    value4.foreach(println)





  }

}
