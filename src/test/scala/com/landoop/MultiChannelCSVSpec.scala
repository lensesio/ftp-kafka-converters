package com.landoop

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class MultiChannelCSVSpec extends WordSpec with Matchers {

  "MultiChannelCSV" should {

    val sourcePartition = Map("0" -> 0L).asJava
    val sourceOffset = Map("filename" -> 0L).asJava

    "be able to get Snapshot records and Channel A records only" in {

      val line =
        """ABCDE123,11313710,00:10:47 Sat 24/10/2015,7039711.194,4697.13,0.000000,0.000000,1.100000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000161,0.132475,0.001060,0.287760,0.000000,0.944650,0.000000,2.056450,0.000000,1.474450,0.000000,1.899050,0.000000,2.537750,0.000000,3.042050,0.000000,1.558050,0.000000,1.088600,0.000000,1.885050,0.000000,1.490950,0.000000,1.949250,0.000000,1.901000,0.000000,1.609150,0.000000,1.782250,0.000000,1.156200,0.000000,1.592900,0.000000,0.471145,0.000000,0.080570,0.015202,0.000003,0.047611,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000,0.000000"""

      val inputLineRecord = new SourceRecord(sourcePartition, sourceOffset, "topic", 0, null, line.getBytes)

      val convertedRecords = new MultiChannelCSV().convert(inputLineRecord).asScala

      convertedRecords.size shouldBe 48

      // For every 1 CSV line this emits one record on one topic - and a sequence of messages on another topic
      //convertedRecords.head.topic shouldBe "topic-snapshots"
      //val expected = ChannelASnapshot(deviceID = "ABCDE123", meterID = "11313710", 1445641847L, snapshot = 7039711.194D).getStructure
      // convertedRecords.head.value.asInstanceOf[Struct] shouldBe expected

      // 1nd record .. 49th is 30 minute separated sequence of messages
      val channelARecord = convertedRecords(0).value.asInstanceOf[Struct]
      channelARecord shouldBe ChannelA(deviceID = "ABCDE123", meterID = "11313710", epoch = 1445641847L, measurement = 0.0D).getStructure

      // 2rd record on 1.1
      val channelARecordTwo = convertedRecords(1).value.asInstanceOf[Struct]
      channelARecordTwo shouldBe ChannelA(deviceID = "ABCDE123", meterID = "11313710", epoch = 1445645447L, measurement = 1.1D).getStructure

    }

  }

}
