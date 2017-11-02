package com.datamountaineer.streamreactor.connect.ftp.source

import org.apache.kafka.common.Configurable
import org.apache.kafka.connect.source.SourceRecord

// We intentionally keep this file in this package, to match expected run-time reflection
trait SourceRecordConverter extends Configurable {
  def convert(in:SourceRecord) : java.util.List[SourceRecord]
}
