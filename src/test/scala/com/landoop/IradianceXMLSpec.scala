package com.landoop

import org.apache.kafka.connect.data.Struct
import org.apache.kafka.connect.source.SourceRecord
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConverters._
import scala.xml.Elem

/**
  *
  * `irradiance` is the rate of the solar power delivered on an area at some point in the world
  *
  * We care about:
  * 1- The site id
  * 2- the latitude
  * 3- the longitude
  * 4- all the date-time values
  */
class IradianceXMLSpec extends WordSpec with Matchers {

  "IradianceXML" should {

    val sourcePartition = Map("0" -> 0L).asJava
    val sourceOffset = Map("filename" -> 0L).asJava

    "flatten xml into records with ( site id, lat, lng, dateTime, value )" in {

      val inputLineRecord = new SourceRecord(sourcePartition, sourceOffset, "topic", 0, null, sample.toString.getBytes)

      val convertedRecords = new IradianceXML().convert(inputLineRecord).asScala

      convertedRecords.size shouldBe 96
      val expectedValue = IradianceData(siteID = "site-ID-1234", lat =56.3491D , lng = -2.41118D,datetime="2017-01-21T09:18:00.000Z", value=132.0D).getStructure
      convertedRecords(36).value.asInstanceOf[Struct] shouldBe expectedValue

    }

  }

  val sample: Elem =
    <SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/">
      <SOAP-ENV:Header>
        <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd"
                       xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd" SOAP-ENV:mustUnderstand="1">
          <wsu:Timestamp wsu:Id="TS-2948">
            <wsu:Created>2017-01-21T00:13:11.807Z</wsu:Created>
            <wsu:Expires>2017-01-21T00:18:11.807Z</wsu:Expires>
          </wsu:Timestamp>
          <wsse11:SignatureConfirmation xmlns:wsse11="http://docs.oasis-open.org/wss/oasis-wss-wssecurity-secext-1.1.xsd" wsu:Id="SC-2947"/>
        </wsse:Security>
      </SOAP-ENV:Header>
      <SOAP-ENV:Body>
        <ns2:dataDeliveryResponse xmlns:ns2="http://geomodel.eu/schema/ws/data"
                                  xmlns:ns3="http://geomodel.eu/schema/common/geo">
          <ns2:site id="site-ID-1234" lat="56.3491" lng="-2.41118">
            <ns2:metadata>
              Usually lots of text , describing the location, the elevation, assistive text, documentation
              urls etc regarding the particular data point
            </ns2:metadata>
            <ns2:columns>GTI</ns2:columns>
            <ns2:row dateTime="2017-01-21T00:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T00:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T00:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T01:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T01:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T01:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T01:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T02:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T02:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T02:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T02:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T03:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T03:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T03:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T03:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T04:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T04:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T04:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T04:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T05:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T05:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T05:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T05:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T06:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T06:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T06:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T06:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T07:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T07:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T07:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T07:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T08:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T08:18:00.000Z" values="1.0"/>
            <ns2:row dateTime="2017-01-21T08:33:00.000Z" values="17.0"/>
            <ns2:row dateTime="2017-01-21T08:48:00.000Z" values="62.0"/>
            <ns2:row dateTime="2017-01-21T09:03:00.000Z" values="95.0"/>
            <ns2:row dateTime="2017-01-21T09:18:00.000Z" values="132.0"/>
            <ns2:row dateTime="2017-01-21T09:33:00.000Z" values="165.0"/>
            <ns2:row dateTime="2017-01-21T09:48:00.000Z" values="198.0"/>
            <ns2:row dateTime="2017-01-21T10:03:00.000Z" values="231.0"/>
            <ns2:row dateTime="2017-01-21T10:18:00.000Z" values="259.0"/>
            <ns2:row dateTime="2017-01-21T10:33:00.000Z" values="282.0"/>
            <ns2:row dateTime="2017-01-21T10:48:00.000Z" values="302.0"/>
            <ns2:row dateTime="2017-01-21T11:03:00.000Z" values="321.0"/>
            <ns2:row dateTime="2017-01-21T11:18:00.000Z" values="334.0"/>
            <ns2:row dateTime="2017-01-21T11:33:00.000Z" values="344.0"/>
            <ns2:row dateTime="2017-01-21T11:48:00.000Z" values="352.0"/>
            <ns2:row dateTime="2017-01-21T12:03:00.000Z" values="352.0"/>
            <ns2:row dateTime="2017-01-21T12:18:00.000Z" values="352.0"/>
            <ns2:row dateTime="2017-01-21T12:33:00.000Z" values="347.0"/>
            <ns2:row dateTime="2017-01-21T12:48:00.000Z" values="342.0"/>
            <ns2:row dateTime="2017-01-21T13:03:00.000Z" values="334.0"/>
            <ns2:row dateTime="2017-01-21T13:18:00.000Z" values="323.0"/>
            <ns2:row dateTime="2017-01-21T13:33:00.000Z" values="305.0"/>
            <ns2:row dateTime="2017-01-21T13:48:00.000Z" values="287.0"/>
            <ns2:row dateTime="2017-01-21T14:03:00.000Z" values="256.0"/>
            <ns2:row dateTime="2017-01-21T14:18:00.000Z" values="206.0"/>
            <ns2:row dateTime="2017-01-21T14:33:00.000Z" values="168.0"/>
            <ns2:row dateTime="2017-01-21T14:48:00.000Z" values="153.0"/>
            <ns2:row dateTime="2017-01-21T15:03:00.000Z" values="119.0"/>
            <ns2:row dateTime="2017-01-21T15:18:00.000Z" values="71.0"/>
            <ns2:row dateTime="2017-01-21T15:33:00.000Z" values="33.0"/>
            <ns2:row dateTime="2017-01-21T15:48:00.000Z" values="7.0"/>
            <ns2:row dateTime="2017-01-21T16:03:00.000Z" values="6.0"/>
            <ns2:row dateTime="2017-01-21T16:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T16:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T16:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T17:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T17:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T17:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T17:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T18:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T18:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T18:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T18:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T19:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T19:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T19:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T19:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T20:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T20:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T20:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T20:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T21:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T21:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T21:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T21:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T22:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T22:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T22:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T22:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T23:03:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T23:18:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T23:33:00.000Z" values="0.0"/>
            <ns2:row dateTime="2017-01-21T23:48:00.000Z" values="0.0"/>
            <ns2:row dateTime="2016-12-02T00:03:00.000Z" values="0.0"/>
          </ns2:site>
        </ns2:dataDeliveryResponse>
      </SOAP-ENV:Body>
    </SOAP-ENV:Envelope>

}
