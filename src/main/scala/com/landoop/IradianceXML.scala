package com.landoop

import scala.collection.JavaConverters._
import java.util

import com.datamountaineer.streamreactor.connect.ftp.source.SourceRecordConverter
import org.apache.kafka.connect.source.SourceRecord

class IradianceXML extends SourceRecordConverter {

  override def configure(props: util.Map[String, _]): Unit = {}

  override def convert(in: SourceRecord): util.List[SourceRecord] = {
    val line = new String(in.value.asInstanceOf[Array[Byte]])
    val data = scala.xml.XML.loadString(line)

    val siteID = (data \\ "site" \ "@id").toString
    val lat = (data \\ "site" \ "@lat").toString.toDouble
    val lng = (data \\ "site" \ "@lng").toString.toDouble

    val rows = (data \\ "row").map { rowData =>
      val dateTime = (rowData \ "@dateTime").toString
      val value = (rowData \ "@values").toString.toDouble
      val message = IradianceData(siteID, lat, lng, dateTime, value)
      new SourceRecord(in.sourcePartition, in.sourceOffset, in.topic, 0, message.connectSchema, message.getStructure)
    }
    rows.toList.asJava
  }

}
