package com.landoop

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._

class HorizontalMonthlyCSVSpec extends WordSpec with Matchers {

  "HorizontalMonthlyCSV" should {

    val sourcePartition = Map("0" -> 0L).asJava
    val sourceOffset = Map("filename" -> 0L).asJava

    "parse a line with 48 daily metrics" in {

      val line =
        """ABCDEFG_214669932_Import,21/01/2017,1.5,1.5,1.6,1.5,1.5,1.5,1.7,1.6,1.5,2,10.2,10.4,10.2,12.6,11.2,9.5,8.8,8.9,3.9,0.4,1.2,1.4,1.1,5.3,3.5,7,3,0.2,1.2,1.9,2.9,0,0,0,0,0.1,0.8,1.5,1.4,1.5,1.6,1.5,1.4,1.5,1.7,1.4,1.5,1.5"""

      val inputLineRecord = new SourceRecord(sourcePartition, sourceOffset, "topic", 0, null, line.getBytes)

      val convertedRecords = new HorizontalMonthlyCSV().convert(inputLineRecord)

      convertedRecords.size shouldBe 48
      convertedRecords.get(0).value.asInstanceOf[Struct] shouldBe  DeviceEvent("ABCDEFG_214669932_Import", 1484956860L, 1.5D).getStructure
      convertedRecords.get(2).value.asInstanceOf[Struct] shouldBe DeviceEvent("ABCDEFG_214669932_Import", 1484960460L, 1.6D).getStructure
      convertedRecords.get(6).value.asInstanceOf[Struct] shouldBe DeviceEvent("ABCDEFG_214669932_Import", 1484967660L, 1.7D).getStructure

    }

    "allow missing reading" in {

      val line =
        """ABCDEFG_214669932_Import,21/01/2017,1.4,1.3,1.7,1.3,1.5,,,,,,,,12.5,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,"""

      val inputLineRecord = new SourceRecord(sourcePartition, sourceOffset, "topic", 0, null, line.getBytes)

      val convertedRecords = new HorizontalMonthlyCSV().convert(inputLineRecord)
      convertedRecords.size shouldBe 6

    }

  }

}
