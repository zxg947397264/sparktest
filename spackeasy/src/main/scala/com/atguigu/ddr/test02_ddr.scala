package com.atguigu.ddr

import org.apache.spark.{SparkConf, SparkContext}

object test02_ddr {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test02_ddr")
    val sc = new SparkContext(conf)

    //    val value: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6),3)
    //
    //    //val unit: RDD[Int] = value.mapPartitions(iter=> iter.filter(_==1))
    //     val unit: RDD[(Int, Int)] = value.mapPartitionsWithIndex((index,iter)=> iter.map((index,_)))
    //    unit.foreach(println)
    //  val value: RDD[Int] = sc.makeRDD(Array(1,2,3,4,5,6),3)
    ////    val abc: RDD[Any] = value.flatMap {
    ////      case list: List[_] => list
    ////      case list: Int => List(list)
    ////    }
    //     value.mapPartitionsWithIndex((index,iter)=>Iterator(index,iter.max)).foreach(println)
    //

    //     val value: RDD[Int] = sc.makeRDD(Array(1,2,3,2,2,3,4,5,6),3)
    //    val unit: RDD[Array[Int]] = value.glom()
    //    val value1: RDD[Array[Int]] = unit.filter(_.length==1)
    //    val unit1: RDD[Int] = value.distinct(2)
    //    unit1.collect().foreach(println)
    //   value1.foreach(a=>{
    //     a.foreach(a=>print(a+" "))
    //    println()
    //   })
//    val value: RDD[String] = sc.makeRDD(Array("a", "b", "c", "d"), 2)
//    val unit: RDD[Array[String]] = value.glom()
//    val unit: RDD[(String, Int)] = sc.makeRDD(List(
//      ("a", 1), ("a", 2), ("c", 3),
//      ("b", 4), ("c", 5), ("c", 6)
//    ), 2)
//    unit.aggregateByKey((0,0))((acc, v) => (acc._1 + v, acc._2 + 1),
//    (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    val tuples = List(("Hello Scala Spark World", 4), ("Hello Scala Spark", 3), ("Hello Scala", 2), ("Hello", 1))

    // (Hello,4),(Scala,4),(Spark,4),(World,4)
    // (Hello,3),(Scala,3),(Spark,3)
    // (Hello,2),(Scala,2)
    // (Hello,1)
    val wordToCountList: List[(String, Int)] = tuples.flatMap {
      t => {
        val strings: Array[String] = t._1.split(" ")
        strings.map(word => (word, t._2))
      }
    }

    // Hello, List((Hello,4), (Hello,3), (Hello,2), (Hello,1))
    // Scala, List((Scala,4), (Scala,3), (Scala,2)
    // Spark, List((Spark,4), (Spark,3)
    // Word, List((Word,4))
    val wordToTupleMap: Map[String, List[(String, Int)]] = wordToCountList.groupBy(t=>t._1)
    println(wordToTupleMap)





  }
}
