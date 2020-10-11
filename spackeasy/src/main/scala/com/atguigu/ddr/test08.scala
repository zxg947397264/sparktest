package com.atguigu.ddr

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test08 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test02_ddr")
    val sc = new SparkContext(conf)
    val vlaue: RDD[String] = sc.textFile("./user_visit_action.txt")

    val value: RDD[(String, (Int, Int, Int))] =
      vlaue.flatMap {
      a =>
        val strings: Array[String] = a.split("_")
        if (strings(6) != "-1") {
          List((strings(6), (1, 0, 0)))
        } else if (strings(8) != "null") {
          val strings1: Array[String] = strings(8).split(",")
          strings1.map(b => (b, (0, 1, 0)))
        } else if(strings(10) != "null"){
          val strings1: Array[String] = strings(10).split(",")
          strings1.map(c => (c, (0, 0, 1)))
        }else Nil
    }

    val value1: RDD[(String, (Int, Int, Int))] = value.reduceByKey {
      case ((a1, b1, c1), (a2, b2, c2)) =>
        (a1 + a2, b1 + b2, c1 + c2)
    }
    value1.sortBy(a=>a._2,false)
      .foreach(println)
  }

}
