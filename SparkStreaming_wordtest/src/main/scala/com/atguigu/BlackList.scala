package com.atguigu

import java.sql.Connection
import java.text.SimpleDateFormat
import java.util.Properties

import com.atguigu.Utils.{JdbcUtil, PropertiesUtil}
import com.atguigu.been.{Ads_log, Evy_log}
import com.atguigu.handler.BlackHandler
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object BlackList {
  def main(args: Array[String]): Unit = {
    //创建SparkConf
    val sc: SparkConf = new SparkConf().setMaster("local[*]").setAppName("test11")
    //创建SparkStreaming
    val ssc = new StreamingContext(sc, Seconds(3))
    //消费kafka数据
    val properties: Properties = PropertiesUtil.load("config.properties")
    val Kafkastream: InputDStream[ConsumerRecord[String, String]] =
      MyKafkaUtil.getKafkaStream(properties.getProperty("kafka.topic"), ssc)
    print("初始化完成")

Kafkastream.transform {rdd=>
  // 找出黑名单中的ID
  val connection: Connection = JdbcUtil.getConnection
  val BlackID: ListBuffer[String] = JdbcUtil.getBlackListFromMysql(connection,
    """
      |select
      |   userid
      |from
      |black_list
      |""".stripMargin,
    Array())
  connection.close()

  //广播变量
  val value: Broadcast[ListBuffer[String]] = ssc.sparkContext.broadcast(BlackID)
}
    //将每行数据转换为样例类数据  ads_log(时间戳，地区，城市，用户id，广告id)
    val ads: DStream[Ads_log] = Kafkastream.map(a => {
      val ss: Array[String] = a.value().split(" ")
      Ads_log(ss(0).toLong, ss(1), ss(2), ss(3), ss(4))
    })
      // 根据黑名单过滤数据
     .filter(log => !value.value.contains(log.userid.toString))

    //value.value.foreach(print)

    //计算当前用户的点击次数
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val adct: DStream[((String, String, String), Int)] = ads.map(log => {
      val timestamp: Long = log.timestamp
      val data: String = sdf.format(timestamp)
      ((data, log.userid, log.adid), 1)
    }).reduceByKey(_ + _)

    //方案二统计城市广告单天次数
//    val area_city_ad: DStream[Evy_log] = ads.map {
//      log =>
//        Evy_log(log.timestamp, log.area, log.city, log.adid)
//    }

//    val dayct: DStream[((String, String, String, String), Int)] = area_city_ad.map {
//      log => {
//        val data: String = sdf.format(log.timestamp)
//        ((data, log.area, log.city, log.adid), 1)
//      }
//    }.reduceByKey(_+_)

    adct.foreachRDD(rdd=>rdd.foreach(print))
    //把数据传输到MYSQL
    BlackHandler.saveBlackList(adct)
   // BlackHandler.saveDayct(dayct)

    //开始并阻塞
    ssc.start()
    ssc.awaitTermination()
  }
}
