package com.scz.user_behavior

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import net.ipip.ipdb.City
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{ConnectionPool, DB, _}

/**
  * 按地区分组统计每日新增VIP数量
  */
object VIPIncrementAnalysis {
  // 提取公共变量，转换算子共用
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  // 从prop文件获取各种参数
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader.getResourceAsStream("VIPIncrementAnalysis.properties"))

  // 使用静态ip资源库
  val ipdb = new City(this.getClass.getClassLoader.getResource("ipipfree.ipdb").getPath)

  // 获取JDBC相关参数
  val driver = prop.getProperty("jdbc.driver")
  val jdbcUrl = prop.getProperty("jdbc.driver")
  val jdbcUser = prop.getProperty("jdbc.user")
  val jdbcPassword = prop.getProperty("jdbc.password")
  // 设置JDBC
  Class.forName(driver)
  // 设置连接池
  ConnectionPool.singleton(jdbcUrl, jdbcUser, jdbcPassword)

  def main(args: Array[String]): Unit = {
    // 参数检测
    if (args.length != 1){
      println("Usage:Please input chechpointPath.")
      System.exit(1)
    }
    // 通过传入参数设置检查点
    val checkPoint = args(0)
    // 通过getOrCreate方式可以实现从Driver端失败恢复
    val ssc = StreamingContext.getOrCreate(checkPoint, () => getVipIncrementByCountry(checkPoint))
    ssc.sparkContext.setLogLevel("ERROR")

    // 启动流计算
    ssc.start()
    ssc.awaitTermination()
  }

  def getVipIncrementByCountry(checkPoint: String): StreamingContext = {
    // 定义update函数
    val updateFun = (value: Seq[Int], state: Option[Int]) => {
      // 本批次value求和
      val currentCount = value.sum
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    // 设置批处理间隔
    val processingInterval = prop.getProperty("processingInterval").toLong
    // 获取kafka相关参数
    val brokers = prop.getProperty("brokers")

    val sparkConf = new SparkConf()
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .setAppName(this.getClass.getSimpleName)
    val ssc = new StreamingContext(sparkConf, Seconds(processingInterval))

    val kafkaParams = Map[String, String] (
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "largest"
    )

    // 获取offset
    val fromOffsets = DB.readOnly(implicit session => sql"select topic,part_id,offset from topic_offset".map(
      r => new TopicPartition(r.string(1), r.int(2)) -> r.long(3)
      ).list().apply().toMap
    )

    var offsetRanges: Array[OffsetRange] = Array.empty[OffsetRange]

    // 获取DStream
    val message = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))

    // 开启检查点
    ssc.checkpoint(checkPoint)
    message.checkpoint(Seconds(processingInterval * 10))

    // 业务计算
    message.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).filter(msg => {
      // 过滤非完成订单的数据，且验证合法性
      filterCompleteOrderData(msg)
    }).map(msg => {
      // 数据转换，返回(("2019-04-02","北京"),1)格式的数据
      getCountryAndData(msg)
    }).updateStateByKey[Int](
      // 更新状态变量
      updateFun
    ).filter(state => {
      // 只保留最近两天的数据，这样返回Driver的数据会非常小
      filter2DaysBeforeState(state)
    }).foreachRDD(rdd => {
      // 将最近两天的数据返回给driver端
      val resultTuple: Array[((String, String), Int)] = rdd.collect()
      // 开启事务
      DB.localTx(implicit session => {
        resultTuple.foreach(msg => {
          val dt = msg._1._1
          val province = msg._1._2
          val cnt = msg._2.toLong
          // 统计结果持久化到MySQL中
          sql"""replace into vip_increment_analysis(province,cnt,dt) values (${province},${cnt},${dt})""".executeUpdate()
          println(msg)
        })
        for (offsetRange <- offsetRanges) {
          println(offsetRange.topic, offsetRange.partition, offsetRange.fromOffset, offsetRange.untilOffset)
          // 保存到offset
          sql"""update topic_offset set offset=${offsetRange.untilOffset} where topic=${offsetRange.topic} and part_id=${offsetRange.partition}"""
        }
      })
    })
    ssc
  }

  /**
    * 过滤非完成订单的数据，且验证数据的合法性
    */
  def filterCompleteOrderData(msg: ConsumerRecord[String, String]): Boolean = {
    var isValid = true
    val fields = msg.value().split("\t")
    // 切分后数据长度不为17，代表数据不合法
    if(fields.length != 17) {
      isValid = false
    } else if(!"completeOrder".equals(fields(15))) {
      isValid = false
    }
    isValid
  }

  /**
    * 数据转换，返回(("2019-04-02","北京"),1)格式的数据
    */
  def getCountryAndData(msg: ConsumerRecord[String, String]): ((String, String), Int) = {
    val fields = msg.value().split("\t")
    // 获取IP地址
    val ip = fields(8)
    // 获取事件时间
    val eventTime = fields(16).toLong
    // 根据日志中的eventTime获取对应的日期
    val date = new Date(eventTime * 1000)
    val eventDay = sdf.format(date)
    // 根据IP获取省份信息
    var regionName = "未知"
    val info = ipdb.findInfo(ip, "CN")
    if(info != null) {
      regionName = info.getRegionName
    }
    ((eventDay, regionName), 1)
  }

  /**
    * 只保留最近两天的状态，因为怕系统时间或系统采集过程中有稍许延迟，所以没有设计之保留一天
    */
  def filter2DaysBeforeState(state: ((String, String), Int)): Boolean = {
    // 获取状态值的对应日期，并转换为13位的长整型时间戳
    val day = state._1._1
    val eventTime = sdf.parse(day).getTime
    // 获取当前系统时间戳
    val currentTime = System.currentTimeMillis()
    // 保留两天之内的
    currentTime - eventTime < 172800000L
  }
}
