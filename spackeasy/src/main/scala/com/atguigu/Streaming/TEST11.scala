package com.atguigu.Streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TEST11 {
  def main(args: Array[String]): Unit = {


    val spconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("zzz")

    val ssc = new StreamingContext(spconf,Seconds(5))
    val kafkaparm: Map[String, Object] = Map[String, Object] {
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092"
      ConsumerConfig.GROUP_ID_CONFIG -> "zxg"
    }

    val unit: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set("test"), kafkaparm))


    val unit1: DStream[(String, Int)] =
      unit.flatMap(a=>a.value().split(" ")).map(a=>(a,1)).reduceByKey(_+_)

    unit1.print()

    ssc.start()
    ssc.awaitTermination()
  }

}
