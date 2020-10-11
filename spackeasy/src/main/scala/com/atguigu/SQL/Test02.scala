package com.atguigu.SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object Test02 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("aaaa")
    val spark: SparkSession = SparkSession.builder()
      .enableHiveSupport()
      .config(sparkConf)
      .getOrCreate()

    spark.sql("use test")

    //     spark.sql(
    //       """
    //         |select
    //         |  c.area ,p.product_name pn,count(*) as alll
    //         |  from
    //         |user_visit_action u
    //         |  join
    //         |city_info c on u.city_id=c.city_id
    //         |  join
    //         |product_info p
    //         |  on
    //         |u.click_product_id = p.product_id
    //         |group by c.area,p.product_name
    //         |"""
    //         .stripMargin ).createTempView("num1")


    spark.sql(
      """
        |select
        |*
        |from
        |business
        |where id not in (select b1.id
        |               from business b1
        |               join business b2
        |               on b1.brand=b2.brand and b1.id!= b2.id
        |               where (b2.startdate<b1.startdate and b1.startdate<b2.enddate)
        |                and (b2.startdate<b1.enddate and b1.enddate<b2.enddate))
        |""".stripMargin).createTempView("num1")

    spark.sql(
      """
        |select *,
        |lag(enddate,1,date_sub(startdate,1)) over(partition by brand order by startdate) ed
        |from
        |num1
        |""".stripMargin).createTempView("num2")

    spark.sql(
      """
        |select brand ,
        |if(startdate>ed,startdate,date_add(ed,1)) newstartdate,
        |enddate
        |from
        |num2
        |""".stripMargin).createTempView("num3")

    spark.sql(
      """
        |select brand,
        |datediff(enddate, newstartdate)+1 day
        |from
        |num3
        |""".stripMargin).createTempView("num4")

    spark.sql(
      """
        |select brand ,sum (day) saleallday
        |from num4
        |group by brand
        |""".stripMargin).show()


  }

}
