

import kafka.common.TopicAndPartition
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object test0_8Directstream {

  def main(args: Array[String]): Unit = {

    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("zaza")
    val ssc = new StreamingContext(sconf, Seconds(5))

    val kafkaparm: Map[String, String] = Map[String, String] {
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop103:9092"
      ConsumerConfig.GROUP_ID_CONFIG -> "zxg1"
    }

    val partitionToLong: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long] {
      TopicAndPartition("test", 0) -> 11
    }

    //val unit: InputDStream[(String, String)] = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaparm,Set("test"))
   // KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaparm,partitionToLong,)

  }
}
