package com.scz.advertising

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by shen on 2020/2/27.
  */
object MockRealTimeData {
  /**
    * 模拟的数据
    * 时间点: 当前时间毫秒
    * userId: 0 - 99
    * 省份、城市 ID相同 ： 1 - 9
    * adid: 0 - 19
    * ((0L,"北京","北京"),(1L,"上海","上海"),(2L,"南京","江苏省"),(3L,"广州","广东省"),(4L,"三亚","海南省"),(5L,"武汉","湖北省"),(6L,"长沙","湖南省"),(7L,"西安","陕西省"),(8L,"成都","四川省"),(9L,"哈尔滨","东北省"))
    * 格式 ：timestamp province city userid adid
    * 某个时间点 某个省份 某个城市 某个用户 某个广告
    */
  def generateMockData(): Array[String] = {
    val array = ArrayBuffer[String]()
    val random = new Random()
    // 模拟实时数据：
    // timestamp province city userid adid
    for (i <- 0 to 50) {

      val timestamp = System.currentTimeMillis()
      val province = random.nextInt(10) + 1
      val city = province
      val adid = random.nextInt(20) + 1
      val userid = random.nextInt(3) + 1

      // 拼接实时数据
      array += timestamp + " " + province + " " + city + " " + userid + " " + adid
    }
    array.toArray
  }

  def getAdReadTimeValueDStream(streamingContext : StreamingContext): ReceiverInputDStream[String] = {
    val adReadTimeValueDStream = streamingContext.receiverStream(new Receiver[String](StorageLevel.MEMORY_ONLY) {
      override def onStart(): Unit = {
        var i = 0
        while (i < 200) {
          for (item <- MockRealTimeData.generateMockData()) {
            store(item)
          }
          Thread.sleep(500)
          i += 1
        }
      }
      override def onStop(): Unit = {}
    })
    adReadTimeValueDStream
  }
}
