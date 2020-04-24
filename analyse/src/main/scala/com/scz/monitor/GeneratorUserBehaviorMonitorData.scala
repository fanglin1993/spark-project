package com.scz.monitor

import java.io.PrintWriter
import java.text.SimpleDateFormat

/**
  * Created by shen on 2020/3/1.
  */
object GeneratorUserBehaviorMonitorData {

  def main(args: Array[String]):Unit ={
    val args = Array("2019-04-30")
    if(args.length != 1) {
      println("Usage:Please input date like 2019-04-02")
      System.exit(1)
    }
    generatorMonitorData(args(0))
  }

  def generatorMonitorData(date: String): Unit = {
    // 初始化手机号前6位，后5位自动化补齐
    val initPhone=187014

    // 初始化时间缀，精确到秒
    val sdf=new SimpleDateFormat("yyyy-MM-dd")
    val eventTime=sdf.parse(date)
    val initTimestamp=eventTime.getTime() / 1000

    // 生成看视频但是没有看完的数据
    writeMonitorData2File("watchVideo", false, 10000, 10000, initPhone, initTimestamp)
    //生成看视频且看完的数据
    writeMonitorData2File("completeVideo", true, 20001, 8000, initPhone, initTimestamp)
    // 生成看完视频且开始做作业的数据
    writeMonitorData2File("startHomework", true, 30001, 7000, initPhone, initTimestamp)
    // 生成看完视频且做完作业的数据
    writeMonitorData2File("completeHomework", true, 40001, 6000, initPhone, initTimestamp)
    // 生成进入订单页的数据
    writeMonitorData2File("enterOrderPage", true, 50001, 4000, initPhone, initTimestamp)
    // 生成进入订单页且完成订单的数据
    writeMonitorData2File("completeOrder", true, 60000, 2000, initPhone, initTimestamp)
  }

  def writeMonitorData2File(dataType: String, isCompleteVideo: Boolean, initUid: Int, userAccount: Int, initPhone: Int, initTimestamp: Long) = {
    val writer: PrintWriter = new PrintWriter(s"./data/${dataType}_${initTimestamp}.txt")
    // 获取开始看视频时间，结束看视频时间和事件发生时间
    val (startVideoTime, endVideoTime, eventTime) = getVideoTimeAndEventTime(initTimestamp, isCompleteVideo)
    for(uid <- initUid until (initUid + userAccount)) {
      // 拼接完整的11位手机号
      val phone = initPhone + "" + uid
      val event=dataType match {
        case "watchVideo" =>
          s"""
             |$uid\t$uid\tF\t2\t0\tSymbian\tauto\t4G\t27.129.32.0\t$phone\t1\t300\t$startVideoTime\t0\t1.0\tstartVideo\t$eventTime
             |$uid\t$uid\tF\t2\t0\tSymbian\tauto\t4G\t27.129.32.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.0\tsendVideo\t$eventTime\n
           """.stripMargin
        case "completeVideo" =>
          s"""
             |$uid\t$uid\tM\t1\t0\tios\tauto\twifi\t59.48.116.0\t$phone\t0\t0\t0\t0\t1.0\tregisterAccount\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\tauto\twifi\t59.48.116.0\t$phone\t0\t0\t0\t0\t1.0\tstartApp\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\tauto\twifi\t59.48.116.0\t$phone\t1\t300\t$startVideoTime\t0\t1.0\tstartVideo\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\tauto\twifi\t59.48.116.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.0\tendVideo\t$eventTime\n
           """.stripMargin
        case "startHomework" =>
          s"""
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t0\t0\t0\t0\t1.0\tregisterAccount\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t0\t0\t0\t0\t1.0\tstartApp\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t1\t300\t$startVideoTime\t0\t1.0\tstartVideo\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.0\tendVideo\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t1\t0\t0\t0\t1.0\tstartHomework\t$eventTime\n
           """.stripMargin
        case "completeHomework" =>
          s"""
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t0\t0\t0\t0\t1.0\tregisterAccount\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t0\t0\t0\t0\t1.0\tstartApp\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t1\t300\t$startVideoTime\t0\t1.0\tstartVideo\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.0\tendVideo\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t1\t0\t0\t0\t1.0\tstartHomework\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\thuawei\twifi\t59.48.116.0\t$phone\t1\t0\t0\t0\t1.0\tcompleteHomework\t$eventTime\n
           """.stripMargin
        case "enterOrderPage" =>
          s"""
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t59.48.116.0\t$phone\t0\t0\t0\t0\t1.1\tregisterAccount\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t59.48.116.0\t$phone\t0\t0\t0\t0\t1.1\tstartApp\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t59.48.116.0\t$phone\t1\t300\t$startVideoTime\t0\t1.1\tstartVideo\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t59.48.116.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.1\tendVideo\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t59.48.116.0\t$phone\t1\t0\t0\t0\t1.1\tstartHomework\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t59.48.116.0\t$phone\t1\t0\t0\t0\t1.1\tcompleteHomework\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t59.48.116.0\t$phone\t0\t0\t0\t0\t1.1\tenterOrderPage\t$eventTime\n
           """.stripMargin
        case "completeOrder" =>
          s"""
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t42.86.6.0\t$phone\t0\t0\t0\t0\t2.0\tregisterAccount\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t42.86.6.0\t$phone\t0\t0\t0\t0\t2.0\tstartApp\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t42.86.6.0\t$phone\t1\t300\t$startVideoTime\t0\t2.0\tstartVideo\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t42.86.6.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t2.0\tendVideo\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t42.86.6.0\t$phone\t1\t0\t0\t0\t2.0\tstartHomework\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t42.86.6.0\t$phone\t1\t0\t0\t0\t2.0\tcompleteHomework\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t42.86.6.0\t$phone\t0\t0\t0\t0\t2.0\tenterOrderPage\t$eventTime
             |$uid\t$uid\tM\t1\t0\tios\ttoutiao\twifi\t42.86.6.0\t$phone\t0\t0\t0\t0\t2.0\tcompleteOrder\t$eventTime\n
           """.stripMargin
      }
      writer.println(event.trim)
    }
    writer.close()
  }

  def getVideoTimeAndEventTime(initTimestamp: Long, isCompleteVideo: Boolean) = {
    // 定义开始视频时间为传入的initTimestamp
    val startVideoTime = initTimestamp
    // 因为视频时长统一定义为300秒，如果是未完成视频，则endVideoTime统一定义为initTimestamp + 100；如完成，则endVideoTime统一定义为initTimestamp + 300
    val endVideoTime=if(isCompleteVideo) initTimestamp + 300 else initTimestamp + 100
    // 事件发生时间eventTime也统一定义为initTimestamp即可
    val eventTime=initTimestamp
    (startVideoTime, endVideoTime, eventTime)
  }

}
