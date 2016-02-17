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

import org.apache.avro.file.DataFileWriter
import org.apache.avro.reflect.{ReflectData, ReflectDatumWriter}
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.compress.{DefaultCodec, GzipCodec, SnappyCodec}
import org.apache.hadoop.io.{IOUtils, Writable}
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.hdfs.HdfsConfig

/**
  * Created by ebice on 1/29/2016.
  */
class AvroDataFileHdfsWriter (dfs: FileSystem, systemName: String, config: HdfsConfig)
  extends HdfsWriter[DataFileWriter[Object]](dfs, systemName, config) {

  val batchSize = config.getWriteBatchSizeRecords(systemName)
  val bucketer = Some(Bucketer.getInstance(systemName, config))
  var recordsWritten = 0L

  /**
    *  Accepts a human-readable compression type from the job properties file such as
    *  "gzip", "snappy", or "none" - returns an appropriate CompressionCodec.
    */
  def getCompressionCodec(compressionType: String) = {
    compressionType match {
      case "snappy" => new SnappyCodec()
      case "gzip"   => new GzipCodec()
      case _        => new DefaultCodec()
    }
  }

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
    val path = bucketer.get.getNextWritePath(dfs)
    val schema = ReflectData.get().getSchema(record.getClass)
    val datumWriter = new ReflectDatumWriter[Object](schema)
    val fileWriter = new DataFileWriter[Object](datumWriter)
    Some(fileWriter.create(schema, dfs.create(path)))
  }

}
