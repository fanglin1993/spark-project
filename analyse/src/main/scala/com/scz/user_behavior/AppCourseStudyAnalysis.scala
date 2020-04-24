package com.scz.user_behavior

import org.apache.spark.sql.SparkSession

object AppCourseStudyAnalysis {
  def main(args: Array[String]): Unit = {
    // 获取日期并验证
    val day = args(0)
    if ("".equals(day) || day.length != 8) {
      println("Usage:Please input date,eg:20190402")
      System.exit(1)
    }
    // 获取SparkSession，并支持Hive操作。
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
//      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.sql.shuffle.partitions", 1)
      .enableHiveSupport()
      .master("local[2]")
      .getOrCreate()

    // 创建临时表
    spark.sql(
      s"""
         |create table if not exists tmp.app_cource_study_analysis_${day}(
         |watch_video_count INT,
         |complete_video_count INT,
         |dt INT
         |) row format delimited fields terminated by "\t";
       """.stripMargin)

    // 将分析结果插入临时表
    val df = spark.sql(
      s"""
         |insert overwrite table tmp.app_cource_study_analysis_${day}
         |select sum(watch_video_count),sum(complete_video_count),dt from(
         |select count(distinct uid) as watch_video_count,0 as complete_video_count,dt from dwd.user_behavior where dt=${day} and event_key="startVideo" group by dt
         |union all
         |select 0 as watch_video_count,count(distinct uid) as complete_video_count,dt from dwd.user_behavior where dt=${day} and event_key="endVideo" and(end_video_time - start_video_time)>=video_length group by dt) tmp
         |group by dt
       """.stripMargin)
    spark.stop()
  }
}
