package com.landoop

import java.util

import org.apache.kafka.connect.source.SourceRecord
import scala.collection.JavaConverters._

case class IradianceData(siteID: String, lat: Double, lng: Double, datetime: String, value: Double)

class IradianceXML extends SourceRecordConverter {

  override def configure(props: util.Map[String, _]): Unit = {}

  override def convert(in: SourceRecord): util.List[SourceRecord] = {
    val line = in.value.toString
    val data = scala.xml.XML.loadString(line)

    val siteID = (data \\ "site" \ "@id").toString
    val lat = (data \\ "site" \ "@lat").toString.toDouble
    val lng = (data \\ "site" \ "@lng").toString.toDouble

    println(s"siteID = $siteID  lat= $lat  lng= $lng")

    val rows = (data \\ "row").map { rowData =>
      val dateTime = (rowData \ "@dateTime").toString
      val value = (rowData \ "@values").toString.toDouble

      val message = IradianceData(siteID, lat, lng, dateTime, value)
      println("dateTime - " + dateTime + " --> " + message)
      new SourceRecord(in.sourcePartition, in.sourceOffset, in.topic, 0, null, message)
    }

    rows.toList.asJava
  }

}
