package com.landoop

import java.util

import org.apache.kafka.connect.source.SourceRecord
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime

import scala.collection.JavaConverters._

case class ChannelA(deviceID: String, meterID: String, epoch: Long, measurement: Double)

case class ChannelASnapshot(deviceID: String, meterID: String, epoch: Long, snapshot: Double)

class MultiChannelCSV extends SourceRecordConverter {

  override def configure(props: util.Map[String, _]): Unit = {}

  // @formatter:off
  def parseDouble(s: String): Option[Double] = try { Some(s.toDouble) } catch { case _ : Throwable => None }
  // @formatter:on

  val dateFormat = DateTimeFormat.forPattern("H:m:s E d/M/Y")

  override def convert(in: SourceRecord): util.List[SourceRecord] = {
    val line = in.value.toString
    val tokens = Parser.fromLine(line)

    val deviceID = tokens.head
    val meterID = tokens(1)
    val date = DateTime.parse(tokens(2), dateFormat)
    val channelAsnapshot = tokens(3)
    val channelBsnapshot = tokens(4)

    val readings = tokens.drop(5)

    val numOfChannels = 2
    val minutes = 1440 / (readings.length / numOfChannels)

    println("minutes = " + minutes)
    val eventsList = readings.indices.flatMap { index =>
      val value: String = readings(index)
      val parsedDouble = parseDouble(value)
      if (index % 2 == 0 && parsedDouble.isDefined) {
        val newTime = date.plusMinutes(index * minutes).getMillis / 1000
        val event = ChannelA(deviceID, meterID, newTime, parsedDouble.get)
        Option(new SourceRecord(in.sourcePartition, in.sourceOffset, in.topic, 0, null, event))
      }
      else None
    }.toList

    val snapshot = ChannelASnapshot(deviceID, meterID, date.getMillis / 1000, channelAsnapshot.toDouble)
    val snapshotRecord = new SourceRecord(in.sourcePartition(), in.sourceOffset(), in.topic + "-snapshots", 0, null, snapshot)
    (snapshotRecord :: eventsList).asJava

  }

}
