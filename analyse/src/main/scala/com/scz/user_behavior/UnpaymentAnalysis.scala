package com.scz.user_behavior

import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, _}

object UnpaymentAnalysis {

  val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("UnpaymentAnalysis.properties"))
  val driver = prop.getProperty("jdbc.driver")
  val jdbcUrl = prop.getProperty("jdbc.driver")
  val jdbcUser = prop.getProperty("jdbc.user")
  val jdbcPassword = prop.getProperty("jdbc.password")
  Class.forName(driver)
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setAppName(this.getClass.getSimpleName)

    // 设置批处理间隔
    val processingInterval = prop.getProperty("processingInterval").toLong
    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))

    // 获取kafka相关参数
    val brokers = prop.getProperty("brokers")
    val kafkaParams = Map[String, String] (
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false"
    )

    // 获取offset
    val fromOffsets = DB.readOnly(implicit session => sql"select topic,part_id,offset from unpayment_topic_offset".map(
      r => new TopicPartition(r.string(1), r.int(2)) -> r.long(3)
    ).list().apply().toMap
    )

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    // 获取DStream
    val message = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))

    // 业务计算
    message.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).filter(msg => {
      // 过滤非进入订单的数据，且验证合法性
      filterEnterOrderPageData(msg)
    }).map(msg => {
      transformData2Tuple(msg)
    }).reduceByKeyAndWindow((a: Int, b: Int) => a + b, Seconds(processingInterval * 4), Seconds(processingInterval * 2))
      .filter(state => {
        // 过滤订单行为异常的数据
        filterUnnormalOrderUser(state)
      }).foreachRDD(rdd => {
      val resultTuple: Array[((String, String), Int)] = rdd.collect()
      DB.localTx(implicit session => {
        resultTuple.foreach(msg => {
          val uid = msg._1._1
          val phone = msg._1._2
          sql"""replace into unpayment_record(uid,phone) values (${uid},${phone})""".executeUpdate()
        })
        for(offsetRange <- offsetRanges) {
          sql"""update unpayment_topic_offset set offset=${offsetRange.untilOffset} where topic=${offsetRange.topic} and part_id=${offsetRange.partition}"""
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def filterEnterOrderPageData(msg: ConsumerRecord[String, String]): Boolean = {
    var isValid = true
    val fields = msg.value().split("\t")
    if(fields.length != 17) {
      isValid = false
    } else if(!"enterOrderPage".equals(fields(15))) {
      isValid = false
    }
    isValid
  }

  def transformData2Tuple(msg: ConsumerRecord[String, String]): ((String, String), Int) = {
    val fields = msg.value().split("\t")
    val uid = fields(0)
    val phone = fields(9)
    ((uid, phone), 1)
  }

  def filterUnnormalOrderUser(state: ((String, String), Int)): Boolean = {
    var notVip = false
    // 获取用户id
    val uid = state._1._1
    // 获取进入订单的次数
    val cnt = state._2
    // 当进入订单页超过2次时，在业务表查询该用户是否为vip状态
    if(cnt > 2) {
      val result = DB.readOnly(implicit session => {
        sql"""select id from vip_user where uid=${uid}""".map(rs => {
          rs.get[Int](1)
        }).list.apply()
      })
      // 如果结果为空，代表用户还不是vip，所以需要做后续运营
      if(result.isEmpty) {
        notVip = true
      }
    }
    notVip
  }

}
