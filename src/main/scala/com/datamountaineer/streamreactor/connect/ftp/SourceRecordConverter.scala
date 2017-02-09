package com.datamountaineer.streamreactor.connect.ftp

import org.apache.kafka.common.Configurable
import org.apache.kafka.connect.source.SourceRecord

trait SourceRecordConverter extends Configurable {
  def convert(in:SourceRecord) : java.util.List[SourceRecord]
}