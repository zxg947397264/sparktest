package com.atguigu.ddr

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test07 {
  def main(args: Array[String]): Unit = {



    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test02_ddr")
    val sc = new SparkContext(conf)
    val vlaue: RDD[String] = sc.textFile("./user_visit_action.txt")
    val l: Long = System.currentTimeMillis()
    val unit: RDD[(String, String, String)] = vlaue.map {
      a =>
        val strings: Array[String] = a.split("_")
        (strings(strings.length - 7), strings(strings.length - 5), strings(strings.length - 3))
    }

    val unit1: RDD[(String, Array[String], Array[String])] = unit.map {
      a =>
        val strings: Array[String] = a._2.split(",")
        val strings1: Array[String] = a._3.split(",")
        (a._1, strings, strings1)
    }
    val cc: RDD[List[Array[(Int, String)]]] = unit1.map {
      a =>
        val tuples: Array[(Int, String)] = a._2.map(b => (2, b))
        val tuples1: Array[(Int, String)] = a._3.map(b => (3, b))
        List(Array((1, a._1)), tuples, tuples1)
    }
    val unit2: RDD[Array[(Int, String)]] = cc.flatMap(a=>a)
     val unit3: RDD[(Int, String)] = unit2.flatMap(a=>a).filter(a=>a._2!="null" && a._2 != "-1")


    val value: RDD[(Int, Map[String, List[String]])] = unit3.groupByKey().mapValues(a=>a.toList.groupBy(a=>a))
    val value1: RDD[(Int, Map[String, Int])] = value.mapValues(a=>a.map(a=>(a._1,a._2.length)))

    val value2: RDD[Map[String, (Int, Int)]] = value1.map {
      a =>
        a._2.map(cc => (cc._1, (a._1, cc._2)))
    }
    val value3: RDD[(String, (Int, Int))] = value2.flatMap(a=>a)
    val value4: RDD[(String, (Int, Int,Int))] = value3.groupByKey().mapValues(a=>a.toList.sortBy(_._1)).mapValues(a=>((a(0)._2,a(1)._2,a(2)._2))).sortBy(a=>a._2,false,1)
    value4.foreach(println)
    val l1: Long = System.currentTimeMillis()
    println(l1-l)





  }

}
