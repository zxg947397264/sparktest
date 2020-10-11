package com.atguigu.SQL

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

object test03_classtestsql {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName("test")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    val taketwo = new Taketwo
    spark.udf.register("add",functions.udaf(taketwo))

    spark.sql("use test")

    spark.sql(
      """
        |select
        |   c.area,
        |   p.product_name,
        |   c.city_name
        | from
        |user_visit_action u
        |join
        |city_info c
        |join
        |product_info p
        |on
        |u.city_id=c.city_id and p.product_id=u.click_product_id
        |""".stripMargin)
      .createTempView("num1")


    spark.sql(
      """
        |select
        |   area,product_name,
        |   count(product_name) ct,
        |   add(city_name) a
        |from
        |  num1
        |group by area,product_name
        |
        |
        |""".stripMargin)
      .createTempView("num2")


    spark.sql(
      """
        |select
        |area,product_name,ct,a,
        |   row_number() over(partition by area order by ct desc) mc
        |from
        |num2
        |
        |""".stripMargin
    ).createTempView("num3")

    spark.sql(
      """
        |select
        |area,product_name,ct,a
        |from
        |num3
        |where mc<=3
        |""".stripMargin).show()


  }

}

class Taketwo extends Aggregator[String,mutable.HashMap[String,Int],String] {
  override def zero: mutable.HashMap[String, Int] =new mutable.HashMap()

  override def reduce(b: mutable.HashMap[String, Int], a: String): mutable.HashMap[String, Int] = {
    b(a)=b.getOrElse(a,0)+1
    b
  }

  override def merge(b1: mutable.HashMap[String, Int], b2: mutable.HashMap[String, Int]): mutable.HashMap[String, Int] = {
    b2.foreach{
      a=>
        b1(a._1)=b1.getOrElse(a._1,0)+a._2
    }
    b1
  }
  override def finish(reduction: mutable.HashMap[String, Int]): String = {
    val sum: Int = reduction.values.sum
    val tuples: List[(String, Int)] = reduction.toList.sortWith(_._2>_._2).take(2)
    val tuples1: List[(String, Double)] = tuples.map(a=>(a._1,math.round(a._2/sum.toDouble*100D)/100D))
    val ddd: Double = 1-tuples1.map(a=>a._2).sum
    tuples1.mkString(",")+"其他"+ddd.toString
  }

  override def bufferEncoder: Encoder[mutable.HashMap[String, Int]] = Encoders.kryo(classOf[mutable.HashMap[String,Int]])

  override def outputEncoder: Encoder[String] = Encoders.STRING
}