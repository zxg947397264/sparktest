package com.atguigu.ddr

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test04 {
  def main(args: Array[String]): Unit = {
    val l: Long = System.currentTimeMillis()
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test02_ddr")
    val sc = new SparkContext(conf)
    val vlaue: RDD[String] = sc.textFile("./user_visit_action.txt",1)
    val unit: RDD[(String, String, String, String)] = vlaue.map {
      a =>
        val strings: Array[String] = a.split("_")
        (strings(strings.length - 7), strings(strings.length - 6), strings(strings.length - 5), strings(strings.length - 3))
    }
    val value: RDD[(String, String, String, String)] = unit.filter(a=>(! (a._1 == "-1" && a._2 == "-1" && a._3 == "null" && a._4== "null")))
    val click1: RDD[(String, Iterable[String])] = value.map(a=>(a._1,a._2)).groupByKey()
    val click2: RDD[(String, Int)] = click1.map(a=>(a._1,a._2.toList.length)).filter(a=> a._1.toInt>=0)

    val order: RDD[(String, Int)] = value.flatMap(a => a._3.split(",")).filter(a=>a!="null").map(a=>(a,1)).reduceByKey(_+_)

    val pay: RDD[(String, Int)] = value.flatMap(a => a._4.split(",")).filter(a=>a!="null").map(a=>(a,1)).reduceByKey(_+_)
    val unit1: RDD[(String, ((Int, Option[Int]), Option[Int]))] = click2.leftOuterJoin(order).leftOuterJoin(pay)
    val value1: RDD[(String, (Int, Int, Int))] = unit1.mapValues(a => (a._1._1, a._1._2.get, a._2.get))


    val list: List[(String, (Int, Int, Int))] = value1.collect().toList
    val tuples: List[(String, (Int, Int, Int))] = list.sortWith((a, b) => {
      if (a._2._1 != b._2._1) a._2._1 > b._2._1 else if (a._2._2 != b._2._2) a._2._2 > b._2._2 else a._2._3 > b._2._3
    })

    val unit2: RDD[(String, (Int, Int, Int))] = value1.sortByKey(true,1)
    println(tuples)
    val l1: Long = System.currentTimeMillis()
    println(l1-l)


  }

}
