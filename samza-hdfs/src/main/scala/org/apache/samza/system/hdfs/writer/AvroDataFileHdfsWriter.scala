/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.system.hdfs.writer

import org.apache.avro.file.{CodecFactory, DataFileWriter}
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.avro.reflect.{ReflectData, ReflectDatumWriter}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.IOUtils
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.hdfs.HdfsConfig

/**
  * Implentation of HdfsWriter for Avro data files. Stores in a file a sequence of data conforming to a schema.
  * The schema is stored in the file with the data. Each datum in a file is of the same schema.
  * Data is grouped into blocks. A synchronization marker is written between blocks, so that files may be split.
  * Blocks may be compressed.
  */
class AvroDataFileHdfsWriter (dfs: FileSystem, systemName: String, config: HdfsConfig)
  extends HdfsWriter[DataFileWriter[Object]](dfs, systemName, config) {

  val batchSize = config.getWriteBatchSizeRecords(systemName)
  val bucketer = Some(Bucketer.getInstance(systemName, config))
  var recordsWritten = 0L

  override def flush: Unit = writer.map { _.flush }

  override def write(outgoing: OutgoingMessageEnvelope): Unit = {
    val record = outgoing.getMessage
    if (shouldStartNewOutputFile) {
      close
      writer = getNextWriter(record)
    }

    writer.map { seq =>
      seq.append(record)
      recordsWritten += 1
    }
  }

  override def close: Unit = {
    writer.map { w => w.flush ; IOUtils.closeStream(w) }
    writer = None
    recordsWritten = 0L
  }

  protected def shouldStartNewOutputFile: Boolean = {
    recordsWritten >= batchSize || bucketer.get.shouldChangeBucket
  }

  protected def getNextWriter(record: Object): Option[DataFileWriter[Object]] = {
    val path = bucketer.get.getNextWritePath(dfs).suffix(".avro")
    val isGenericRecord = record.isInstanceOf[GenericRecord]
    val schema = record match {
      case genericRecord: GenericRecord => genericRecord.getSchema
      case _ => ReflectData.get().getSchema(record.getClass)
    }
    val datumWriter = if (isGenericRecord)
      new GenericDatumWriter[Object](schema)
    else new ReflectDatumWriter[Object](schema)
    val fileWriter = new DataFileWriter[Object](datumWriter)
    val cn = config.getCompressionType(systemName)
    if (!cn.equals("none")) fileWriter.setCodec(CodecFactory.fromString(cn))
    Some(fileWriter.create(schema, dfs.create(path)))
  }

}
