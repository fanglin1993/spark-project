package com.scz.user_behavior

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UserBehaviorCleaner {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage:please input inputPath and outputPath")
      System.exit(1)
    }
    // 获取输入输出路径
    val inputPath = args(0)
    val outputPath = args(1)
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster("1ocal[*]")
    val sc = new SparkContext(conf)
    // 通过输入路径获取RDD
    val eventRDD: RDD[String] = sc.textFile(inputPath)
    // 清洗数据，在算子中不要写大量业务逻辑，应该将逻辑封到方法中
    eventRDD.filter(event => checkEventValid(event)) //验证数据有效性
      .map { event => maskPhone(event) } //手机号脱敏
      .map(event => repairUsername(event)) //修复username中带有\n导致的换行
      .coalesce(3)
      .saveAsTextFile(outputPath)
    sc.stop()
  }

  /**
    * 验证数据格式是否正确，只有切分后长度为17的才算正确
    *
    * @param event
    * @return
    */
  def checkEventValid(event: String): Boolean = {
    val fields = event.split("\t")
    fields.length == 17
  }

  /**
    * 脱敏手机号
    *
    * @param event
    * @return
    */
  def maskPhone(event: String): String = {
    val maskPhone = new StringBuilder
    val fields = event.split("\t")
    // 取出手机号
    val phone = fields(0)
    // 手机号不为空时做掩码处理
    if (phone != null && !"".equals(phone)) {
      maskPhone.append(phone.substring(0, 3)).append(phone.substring(7, 11))
      fields(9) = maskPhone.toString()
    }
    fields.mkString("\t")
  }

  /**
    * username为用户自定义的，里面有要能存在"\n"，导致写入到HDFS时换行
    *
    * @param event
    * @return
    */
  def repairUsername(event: String): String = {
    val fields = event.split("\t")
    // 取出用户昵称
    val usename = fields(1)
    // 用户昵称不为空时替换"\n"
    if (usename != null && !"".equals(usename)) {
      fields(1) = usename.replace("\n", "")
    }
    fields.mkString("\t")
  }

}
