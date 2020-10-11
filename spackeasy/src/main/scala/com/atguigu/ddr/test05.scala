package com.atguigu.ddr

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test05 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test02_ddr")
    val sc = new SparkContext(conf)
    val vlaue: RDD[String] = sc.textFile("./zxg")
    val unit: RDD[(String, (Int, Int))] = vlaue.map(a => {
      val strings: Array[String] = a.split("\t")
      (strings(1), (strings(strings.length - 3).toInt, strings(strings.length - 2).toInt))
    })
    val unit1: RDD[(String, (Int, Int))] = unit.reduceByKey((a,b)=>((a._1+b._1),(a._2+b._2)))
    val unit2: RDD[(String, (Int, Int, Int))] = unit1.mapValues(a=>(a._1,a._2,(a._1+a._2)))
    unit2.foreach(println)

    val ints = List(Array(1,2,3),Array(1))
    ints.flatMap(a=>a)
  }

}
