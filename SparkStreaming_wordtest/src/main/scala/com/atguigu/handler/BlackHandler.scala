package com.atguigu.handler

import java.sql.Connection

import com.atguigu.Utils.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

object BlackHandler {
  def saveDayct(dayct: DStream[((String, String, String, String), Int)]) = {
    dayct.foreachRDD {
      rdd => {
        rdd.foreachPartition {
          iter =>
            //获取链接
            val connection: Connection = JdbcUtil.getConnection
            //遍历写人MYSQL
            iter.foreach{
              case ((date, area, city, adid), ct) =>
                JdbcUtil.executeUpdate(connection,
                  """
                    |INSERT INTO area_city_ad_count VALUES(?,?,?,?,?)
                    |ON DUPLICATE KEY
                    |UPDATE count=?+count
                    |""".stripMargin,
                  Array(date, area, city, adid, ct, ct))
            }
          connection.close()
        }
      }
    }
  }


  def saveBlackList(adct: DStream[((String, String, String), Int)]): Unit = {

    //分区操作，减少链接次数
    adct.foreachRDD {
      rdd => {

        rdd.foreachPartition { iter =>
          //获取链接
          val connection: Connection = JdbcUtil.getConnection

          //遍历操作数据,将数据写入MYSQL
          iter.foreach {
            case ((date, userid, adid), ct) =>
              JdbcUtil.executeUpdate(connection,
                """
                  |INSERT INTO user_ad_count VALUES(?,?,?,?)
                  |ON DUPLICATE KEY
                  |UPDATE count=?+count
                  |""".stripMargin,
                Array(date, userid, adid, ct, ct)
              )

              //读取MYSQL中的数据

              val count: Long = JdbcUtil.getDataFromMysql(connection,
                """
                  |select
                  |   count
                  |from
                  |   user_ad_count
                  |where
                  |   dt=? and userid=? and adid=?
                  |""".stripMargin,
                Array(date, userid, adid)
              )

              //将大于100的数据放入黑名单
              if (count >= 20) {
                JdbcUtil.executeUpdate(
                  connection,
                  """
                    |INSERT INTO black_list VALUES(?)
                    |ON DUPLICATE KEY
                    |UPDATE userid=?
                    |
                    |""".stripMargin,
                  Array(userid, userid))


//                JdbcUtil.executeUpdate(connection,
//                  """
//                    |delete from user_ad_count
//                    | where userid = ?
//                    |""".stripMargin,
//                  Array(userid))
              }


          }
          //关闭链接
          connection.close()
        }
      }
    }
  }
}


