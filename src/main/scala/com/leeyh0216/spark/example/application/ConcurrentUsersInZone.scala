package com.leeyh0216.spark.example.application

import java.util.concurrent.TimeUnit

import com.leeyh0216.spark.example.log.ZoneInOutLog
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.functions._

class ConcurrentUsersInZone(spark: SparkSession) {
  implicit val sqlContext = spark.sqlContext

  def preProcess(streamDF: DataFrame): DataFrame = {
    streamDF
      //String 타입의 eventTime을 TimeStamp 타입으로 변경
      .withColumn("eventTime", to_timestamp(col("eventTime"), "yyyy-MM-dd HH:mm:ss"))
      //1인 경우 진입이므로 +1, 이외의 경우 퇴장으로 간주하고 -1
      .withColumn("inOutDelta", when(col("eventType") === lit(1), lit(1)).otherwise(lit(-1)))
  }

  def process(streamDF: DataFrame): StreamingQuery = {
    streamDF
      //마지막 Microbatch의 Max(Event Time) - 10초 까지의 데이터를 Watermark
      .withWatermark("eventTime", "10 seconds")
      //1분 단위로 Windowing, Zone으로도 Grouping
      .groupBy(window(col("eventTime"), "1 minutes").as("window"), col("zone"))
      //1분 간의 In/Out Delta 합
      .agg(sum(col("inOutDelta")).as("inOutDelta"))
      //메모리에 결과를 Flush
      .writeStream.format("console")
      .outputMode(OutputMode.Update())
      .queryName("ConcurrentUsersInZone")
      .option("truncate", "false")
      //Watermark 작동 이후에만 결과를 갱신한다.
      //1초마다 반복적으로 Repeated update trigger가 동작하여 계산하도록 합니다.
      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
      .start()
  }
}