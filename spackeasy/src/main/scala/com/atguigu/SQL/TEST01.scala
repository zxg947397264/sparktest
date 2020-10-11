package com.atguigu.SQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

object TEST01{
  def main(args: Array[String]): Unit = {
    //创建配置文件
    val conf: SparkConf = new SparkConf().setAppName("test").setMaster("local[*]")

    //创建session会话
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 隐式转换
    import spark.implicits._

    //读取文件
    val df: DataFrame = spark.read.json("./zxg")


    //封装为DataSet
    val ds: Dataset[User01] = df.as[User01]

    //创建聚合函数
    var myAgeUdaf1 = new Ageave
    //将聚合函数转换为查询的列
    val col: TypedColumn[User01, Double] = myAgeUdaf1.toColumn

    //查询
    ds.select(col).show()
  }
    //输入数据类型
    case class User01(username:String,age:Long)
    //缓存类型
    case class AgeBuffer(var sum:Long,var count:Long)


  case class Ageave() extends Aggregator[User01,AgeBuffer,Double]{


    override def zero: AgeBuffer = AgeBuffer(0L,0L)

    override def reduce(b: AgeBuffer, a: User01): AgeBuffer = {
      b.sum+=a.age
      b.count+=1
      b
    }

    override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
      b1.sum+=b2.sum
      b2.count+=b2.count
      b1
    }

    override def finish(reduction: AgeBuffer): Double = {
      reduction.sum/reduction.count.toDouble
    }

    override def bufferEncoder: Encoder[AgeBuffer] = {
      Encoders.product
    }

    override def outputEncoder: Encoder[Double] = {
      Encoders.scalaDouble
    }
  }



//    /**
//     * 定义类继承org.apache.spark.sql.expressions.Aggregator
//     * 重写类中的方法
//     */
//    class MyAveragUDAF1 extends Aggregator[User01,AgeBuffer,Double]{
//
//      override def zero: AgeBuffer = {
//        AgeBuffer(0L,0L)
//      }
//
//      override def reduce(b: AgeBuffer, a: User01): AgeBuffer = {
//        b.sum = b.sum + a.age
//        b.count = b.count + 1
//        b
//      }
//
//      override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
//        b1.sum = b1.sum + b2.sum
//        b1.count = b1.count + b2.count
//        b1
//      }
//
//      override def finish(buff: AgeBuffer): Double = {
//        buff.sum.toDouble/buff.count
//      }
//      //DataSet默认额编解码器，用于序列化，固定写法
//      //自定义类型就是product自带类型根据类型选择
//      override def bufferEncoder: Encoder[AgeBuffer] = {
//        Encoders.product
//      }
//
//      override def outputEncoder: Encoder[Double] = {
//        Encoders.scalaDouble
//      }
//    }
}
