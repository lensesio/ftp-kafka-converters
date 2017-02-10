package com.landoop

import com.datamountaineer.streamreactor.connect.ftp.SourceRecordConverter
import org.apache.kafka.connect.source.SourceRecord

import scala.collection.JavaConverters._
import org.joda.time.DateTime
import java.util

import com.typesafe.scalalogging.slf4j.StrictLogging
import org.joda.time.format.DateTimeFormat

class MultiChannelCSV extends SourceRecordConverter with StrictLogging {

  override def configure(props: util.Map[String, _]): Unit = {}

  val dateFormat = DateTimeFormat.forPattern("H:m:s E d/M/Y")

  override def convert(in: SourceRecord): util.List[SourceRecord] = {
    val lines = new String(in.value.asInstanceOf[Array[Byte]])
    val allRecords = lines.split("\n").filter(_.length > 10).flatMap { line =>

      val tokens = Parser.fromLine(line)

      println(" line = " + line)
      val deviceID = tokens.head
      val meterID = tokens(1)
      println("Device ID " + deviceID + " meterID = " + meterID)
      val date = DateTime.parse(tokens(2), dateFormat)
      println("date = " + date)
      //val channelAsnapshot = tokens(3)
      //val channelBsnapshot = tokens(4)

      val readings = tokens.drop(5)

      val numOfChannels = 2
      val minutes = 1440 / (readings.length / numOfChannels)

      logger.info(s"MultiChannel CSV parser with 1 entry every $minutes minutes")
      val eventsList = readings.indices.flatMap { index =>
        val value: String = readings(index)
        val parsedDouble = parseDouble(value)
        if (index % 2 == 0 && parsedDouble.isDefined) {
          val newTime = date.plusMinutes(index * minutes).getMillis / 1000
          val event = ChannelA(deviceID, meterID, newTime, parsedDouble.get)
          Option(new SourceRecord(in.sourcePartition, in.sourceOffset, in.topic, 0, event.connectSchema, event.getStructure))
        }
        else None
      }.toList

      //val snapshot = ChannelASnapshot(deviceID, meterID, date.getMillis / 1000, channelAsnapshot.toDouble)
      //val snapshotRecord = new SourceRecord(in.sourcePartition(), in.sourceOffset(), in.topic + "-snapshots", 0, snapshot.connectSchema, snapshot.getStructure)
      //snapshotRecord ::
      eventsList
    }.toList
    allRecords.asJava
  }

  // @formatter:off
  def parseDouble(s: String): Option[Double] = try { Some(s.toDouble) } catch { case _ : Throwable => None }

}
