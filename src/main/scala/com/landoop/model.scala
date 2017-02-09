package com.landoop

import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}

case class IradianceData(siteID: String,
                         lat: Double,
                         lng: Double,
                         datetime: String,
                         value: Double) {

  val connectSchema: Schema = SchemaBuilder.struct()
    .doc("Iradiance Solar Data")
    .name("com.landoop.IradianceData")
    .field("siteID", Schema.STRING_SCHEMA)
    .field("lat", Schema.FLOAT64_SCHEMA)
    .field("lng", Schema.FLOAT64_SCHEMA)
    .field("datetime", Schema.STRING_SCHEMA)
    .field("value", Schema.FLOAT64_SCHEMA)
    .build()

  def getStructure: Struct = new Struct(connectSchema)
    .put("siteID", siteID)
    .put("lat", lat)
    .put("lng", lng)
    .put("datetime", datetime)
    .put("value", value)
}

case class DeviceEvent(deviceID: String,
                       epoch: Long,
                       measurement: Double) {

  val connectSchema: Schema = SchemaBuilder.struct()
    .doc("Horizontal CSV DeviceEvent")
    .name("com.landoop.DeviceEvent")
    .field("deviceID", Schema.STRING_SCHEMA)
    .field("epoch", Schema.INT64_SCHEMA)
    .field("measurement", Schema.FLOAT64_SCHEMA)
    .build()

  def getStructure: Struct = new Struct(connectSchema)
    .put("deviceID", deviceID)
    .put("epoch", epoch)
    .put("measurement", measurement)

}

case class ChannelA(deviceID: String,
                    meterID: String,
                    epoch: Long,
                    measurement: Double) {

  val connectSchema: Schema = SchemaBuilder.struct()
    .doc("ChannelA data from MultiChannel CSV data feed")
    .name("com.landoop.ChannelA")
    .field("deviceID", Schema.STRING_SCHEMA)
    .field("meterID", Schema.STRING_SCHEMA)
    .field("epoch", Schema.INT64_SCHEMA)
    .field("measurement", Schema.FLOAT64_SCHEMA)
    .build()

  def getStructure: Struct = new Struct(connectSchema)
    .put("deviceID", deviceID)
    .put("meterID", meterID)
    .put("epoch", epoch)
    .put("measurement", measurement)

}

case class ChannelASnapshot(deviceID: String,
                            meterID: String,
                            epoch: Long,
                            snapshot: Double) {

  val connectSchema: Schema = SchemaBuilder.struct()
    .doc("ChannelA Snapshot data from MultiChannel CSV data feed")
    .name("com.landoop.ChannelASnapshot")
    .field("deviceID", Schema.STRING_SCHEMA)
    .field("meterID", Schema.STRING_SCHEMA)
    .field("epoch", Schema.INT64_SCHEMA)
    .field("snapshot", Schema.FLOAT64_SCHEMA)
    .build()

  def getStructure: Struct = new Struct(connectSchema)
    .put("deviceID", deviceID)
    .put("meterID", meterID)
    .put("epoch", epoch)
    .put("snapshot", snapshot)

}

