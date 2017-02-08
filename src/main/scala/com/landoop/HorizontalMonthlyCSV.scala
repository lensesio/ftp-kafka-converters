package com.landoop

import java.util

import org.apache.kafka.connect.source.SourceRecord
import org.joda.time.format.{DateTimeFormat, DateTimeFormatter}
import org.joda.time.DateTime

import scala.collection.JavaConverters._

case class Event(id: String, epoch: Long, measurement: Double)

class HorizontalMonthlyCSV extends SourceRecordConverter {

  override def configure(props: util.Map[String, _]): Unit = {}

  var emptyLines = 0L
  var atleastOneEmptyValue = 0L

  // @formatter:off
  def parseDouble(s: String): Option[Double] = try { Some(s.toDouble) } catch { case _ : Throwable => None }
  // @formatter:on

  val dateFormat: DateTimeFormatter = DateTimeFormat.forPattern("dd/mm/yy")

  override def convert(in: SourceRecord): util.List[SourceRecord] = {
    val line = in.value.toString
    val tokens = Parser.fromLine(line)
    val id = tokens.head
    val day = DateTime.parse(tokens(1), dateFormat)
    val readings = tokens.drop(2)

    val minutes = 1440 / readings.length
    println("minutes = " + minutes)
    val eventsList = readings.indices.flatMap { index =>
      val value: String = readings(index)
      val parsedDouble = parseDouble(value)
      if (parsedDouble.isDefined) {
        val newTime = day.plusMinutes(index * minutes).getMillis / 1000
        val event = Event(id, newTime, parsedDouble.get)
        Option(new SourceRecord(in.sourcePartition, in.sourceOffset, in.topic, 0, null, event))
      }
      else None
    }.toList

    if (eventsList.isEmpty)
      emptyLines += 1
    if (eventsList.length != readings.length)
      atleastOneEmptyValue += 1
    if (emptyLines % 100 == 0 || atleastOneEmptyValue % 1000 == 0)
      println(
        s"""
           |Total empty lines processed:     $emptyLines
           |Lines with a missing value :     $atleastOneEmptyValue
        """.stripMargin)

    eventsList.asJava
  }

}
