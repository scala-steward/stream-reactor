/*
 * Copyright 2017-2026 Lenses.io Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.lenses.streamreactor.connect.cloud.common.formats.reader

import io.lenses.streamreactor.connect.avro.AvroDataFactory
import org.apache.avro.Schema
import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.connect.data.SchemaAndValue

import java.io.InputStream
import scala.util.Try

class AvroStreamReader(input: InputStream) extends CloudDataIterator[SchemaAndValue] {
  private val avroDataConverter = AvroDataFactory.create()

  // Avro 1.12.1 enables fastread by default, which decodes DATE logical-type fields
  // as java.time.LocalDate rather than int. Confluent AvroData.toConnectData() expects
  // a raw int for INT32 fields, so we disable fastread on this data model.
  private val model = new GenericData()
  model.setFastReaderEnabled(false)
  private val datumReader = new GenericDatumReader[GenericRecord](null: Schema, null: Schema, model)

  private val stream = new DataFileStream[GenericRecord](input, datumReader)

  override def close(): Unit = {
    val _ = Try(stream.close())
    val _ = Try(input.close())
  }

  override def hasNext: Boolean = stream.hasNext

  override def next(): SchemaAndValue = {
    val genericRecord = stream.next
    avroDataConverter.toConnectData(genericRecord.getSchema, genericRecord)
  }
}
