package com.leeyh0216.spark.example.streaming.log

/**
  * Zone 진입/퇴장 로그 정의
  *
  * @param eventType 1: 진입, 2: 퇴장
  * @param eventTime Event time, Format: yyyy-MM-dd HH:mm:ss
  * @param zone      Zone 이름
  * @param character 캐릭터 UID
  */
case class ZoneInOutLog(eventType: Int, eventTime: String, zone: String, character: Int)
