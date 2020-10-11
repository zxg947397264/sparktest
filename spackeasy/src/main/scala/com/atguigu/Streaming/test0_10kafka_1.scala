package com.atguigu.Streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object test0_10kafka_1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("test0_10kafka_1").setMaster("local[*]")
    val context = new StreamingContext(conf,Seconds(5))

    val zzzz: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "zxg"
    )


    val a1: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      context,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](Set("first"), zzzz))

     val a2: DStream[(String, Int)] = a1.flatMap(a=>a.value().split(" ")).map(a=>(a,1)).reduceByKey(_+_)

    a2.print()


    context.start()

    context.awaitTermination()



  }

}
