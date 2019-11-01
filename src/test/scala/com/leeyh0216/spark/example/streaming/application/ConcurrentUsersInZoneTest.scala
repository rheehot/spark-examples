package com.leeyh0216.spark.example.streaming.application

import com.leeyh0216.spark.example.streaming.log.ZoneInOutLog
import org.apache.spark.sql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.junit.{Assert, Test}

class ConcurrentUsersInZoneTest {
  val spark = new sql.SparkSession.Builder().master("local").appName("ConcurrentUsersInZone").config("spark.driver.host", "localhost").getOrCreate()

  import spark.implicits._

  implicit val sqlContext = spark.sqlContext

  val memoryStream = MemoryStream[ZoneInOutLog]

  val app = new ConcurrentUsersInZone(spark)

  @Test
  def testPreProcess(): Unit = {
    val df = Seq(
      ZoneInOutLog(1, "2019-06-29 13:00:00", "A", 1),
      ZoneInOutLog(2, "2019-06-29 14:00:00", "A", 1)
    ).toDF()

    val answer = Seq(
      (1, "2019-06-29 13:00:00", "A", 1, 1),
      (2, "2019-06-29 14:00:00", "A", 1, -1)
    ).toDF("eventType", "eventTime", "zone", "character", "inOutDelta")

    val expected = app.preProcess(df).select("eventType", "eventTime", "zone", "character", "inOutDelta")

    Assert.assertEquals(0, answer.except(expected).count())
    Assert.assertEquals(0, expected.except(answer).count())
  }

  @Test
  def testProcessWithAppendMode(): Unit = {
    val outputStream = app.process(app.preProcess(memoryStream.toDF()))

    memoryStream.addData(
      ZoneInOutLog(1, "2019-06-29 13:00:00", "A", 1),
      ZoneInOutLog(2, "2019-06-29 13:00:20", "A", 1),
      ZoneInOutLog(1, "2019-06-29 13:00:20", "A", 2),
      ZoneInOutLog(1, "2019-06-29 13:00:45", "A", 1)
    )
    outputStream.processAllAvailable()
    //첫번째 결과 Watermark 적용이 안되기 때문에 빈 테이블이 출력됨
    Assert.assertEquals(0, spark.table("ConcurrentUsersInZone").count())

    memoryStream.addData(
      ZoneInOutLog(2, "2019-06-29 13:00:55", "A", 2),
      ZoneInOutLog(2, "2019-06-29 13:01:11", "A", 1)
    )
    val answer2 = Seq(
      ("A", "2019-06-29 13:00:00", "2019-06-29 13:01:00", 1)
    ).toDF("zone", "start", "end", "inOutDelta")
      .withColumn("start", to_timestamp(col("start"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("end", to_timestamp(col("end"), "yyyy-MM-dd HH:mm:ss"))
    outputStream.processAllAvailable()

    val expected2 = spark.table("ConcurrentUsersInZone").select("zone", "window.start", "window.end", "inOutDelta")
    //두번째 결과
    //2번째 Stream 중 Event Time 이 가장 큰 2019-06-29 13:01:11 기준으로 Watermark 생성(2019-06-29 13:01:01)
    //2번째 Stream을 포함한 이전 Stream에서 Event Time이 13:01:01보다 작은 Window들이 계산됨
    Assert.assertEquals(0, answer2.except(expected2).count())
    outputStream.stop()
  }

  @Test
  def testProcessWithUpdateMode(): Unit = {
    //Output Mode를 Update로 적용
    val outputStream = app.process(app.preProcess(memoryStream.toDF()), outputMode = OutputMode.Update())

    memoryStream.addData(
      ZoneInOutLog(1, "2019-06-29 13:00:00", "A", 1),
      ZoneInOutLog(2, "2019-06-29 13:00:20", "A", 1),
      ZoneInOutLog(1, "2019-06-29 13:00:20", "A", 2),
      ZoneInOutLog(1, "2019-06-29 13:00:45", "A", 1)
    )
    outputStream.processAllAvailable()
    //Watermark가 적용되더라도 Update Mode이기 떄문에 연산 결과가 존재함
    val answer1 = Seq(
      ("A", "2019-06-29 13:00:00", "2019-06-29 13:01:00", 2)
    ).toDF("zone", "start", "end", "inOutDelta")
      .withColumn("start", to_timestamp(col("start"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("end", to_timestamp(col("end"), "yyyy-MM-dd HH:mm:ss"))
    val expected1 = spark.table("ConcurrentUsersInZone").select("zone", "window.start", "window.end", "inOutDelta")
    Assert.assertEquals(0, answer1.except(expected1).count())

    memoryStream.addData(
      ZoneInOutLog(2, "2019-06-29 13:00:55", "A", 2),
      ZoneInOutLog(2, "2019-06-29 13:01:11", "A", 1)
    )
    val answer2 = Seq(
      ("A", "2019-06-29 13:00:00", "2019-06-29 13:01:00", 1),
      ("A", "2019-06-29 13:01:00", "2019-06-29 13:02:00", -1)
    ).toDF("zone", "start", "end", "inOutDelta")
      .withColumn("start", to_timestamp(col("start"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("end", to_timestamp(col("end"), "yyyy-MM-dd HH:mm:ss"))
    outputStream.processAllAvailable()

    val expected2 = spark.table("ConcurrentUsersInZone").select("zone", "window.start", "window.end", "inOutDelta")
    Assert.assertEquals(0, answer2.except(expected2).count())
    outputStream.stop()
  }
}
