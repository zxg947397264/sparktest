package com.atguigu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.{AbstractSeq, mutable}
import scala.collection.mutable.ListBuffer

object LeiJiaQi {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    //3.读取用户行为数据

    val lineRDD: RDD[String] = sc.textFile("./user_visit_action.txt")

    //4.使用累计器计算各个品类点击、订单以及支付数量
    val accu = new countall
    sc.register(accu)

    lineRDD.foreach(line => {

      //分割数据
      val arr: Array[String] = line.split("_")

      //判断是否为点击数据
      if (arr(6) != "-1") {
        accu.add(s"1_${arr(6)}")
      } else if (arr(8) != "null") {
        //订单数据
        arr(8).split(",")
          .foreach(category =>
            accu.add(s"2_$category")
          )
      } else if (arr(10) != "null") {
        //支付数据
        arr(10).split(",")
          .foreach(category =>
            accu.add(s"3_$category")
          )
      }
    })

    println(accu.value)

  }

}

class countall extends AccumulatorV2[String, mutable.HashMap[String, ListBuffer[Int]]] {

   private val map = new mutable.HashMap[String, ListBuffer[Int]]
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, ListBuffer[Int]]] = new countall

  override def reset(): Unit = map.clear()

  override def add(x: String): Unit = {
   val v=x.split("_")(1)
    if(x.split("_")(0) == "1"){
     map(v) = map.getOrElse(v,ListBuffer(0,0,0))
      map(v)(0)=map(v)(0)+1
    }else if (x.split("_")(0)=="2"){
      map(v) = map.getOrElse(v,ListBuffer(0,0,0))
      map(v)(1)=map(v)(1)+1
    }else {
      map(v) = map.getOrElse(v,ListBuffer(0,0,0))
      map(v)(2)=map(v)(2)+1
    }
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, ListBuffer[Int]]]): Unit = {
    other.value.foreach { case (actionCategory, count) =>
      map(actionCategory) = map.getOrElse(actionCategory,ListBuffer(0,0,0))
        map(actionCategory)(0)=map(actionCategory)(0)+count(0)
        map(actionCategory)(1)=map(actionCategory)(1)+count(1)
        map(actionCategory)(2)=map(actionCategory)(2)+count(2)
    }
  }

  override def value: mutable.HashMap[String, ListBuffer[Int]] = map
}


