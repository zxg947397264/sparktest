package com.atguigu.ddr

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object test03 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test02_ddr")
    val sc = new SparkContext(conf)
   val vlaue: RDD[String] = sc.textFile("./agent.log")
    val value1: RDD[((String, String), Int)] = vlaue.map {
      a =>
        val strings: Array[String] = a.split(" ")
        ((strings(1), strings(4)), 1)
    }
    val unit: RDD[((String, String), Int)] = value1.reduceByKey(_+_)

    val value: RDD[(String, Iterable[(String, Int)])] = unit.map(a=>(a._1._1,(a._1._2,a._2))).groupByKey()

    val value2: RDD[(String, List[(String, Int)])] = value.mapValues{
      a=>a.toList.sortWith(_._2>_._2).take(3)
    }
    value2.partitionBy(new HashPartitioner(10))

    value2.saveAsTextFile("./output")

    val dataRDD1 =
      sc.makeRDD(List(("a",1),("a","1"),("b",2),("c",3)))
     val unit1: RDD[(String, Iterable[Any])] = dataRDD1.groupByKey()











//    val unit1: RDD[(String, String)] = unit.map(a=>(a(1),a(4)))
//    val value: RDD[(String, Iterable[String])] = unit1.groupByKey()
//    val value1: RDD[(String, List[String])] = value.map((a)=>(a._1,a._2.toList))
//    val value2: RDD[(String, List[(String, Int)])] = value1.mapValues(a=>a.map(a=>(a,1)))
//    val value3: RDD[(String, Map[(String, Int), List[(String, Int)]])] = value2.mapValues(a=>a.groupBy(a=>a))
//    val value4: RDD[(String, Map[(String, Int), Int])] = value3.mapValues(kv=>kv.map(kv=>(kv._1,kv._2.length)))
//    val value5: RDD[(String, List[((String, Int), Int)])] = value4.mapValues(kv => kv.toList.sortWith((left, right) => {
//      left._2 > right._2
//    }
//    ))
//    val value6: RDD[(String, List[((String, Int), Int)])] = value5.mapValues(k=>k.take(3))
//    val value7: RDD[(String, List[(String, Int)])] = value6.mapValues(a => {
//      a.map(a => ((a._1._1),a._2))
//
//    })
//
//
//    value7.collect().foreach(println)



  }

}
