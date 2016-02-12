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

  val avroClassName = config.getAvroClassName(systemName)
  val avroClass = Class.forName(avroClassName) //.newInstance().asInstanceOf[SpecificRecordBase]
  //val schema = ReflectData.makeNullable(avroClass.getSchema)
  val schema = ReflectData.get().getSchema(avroClass)
  val datumWriter = new ReflectDatumWriter[Object](schema)

  val batchSize = config.getWriteBatchSizeBytes(systemName)
  val bucketer = Some(Bucketer.getInstance(systemName, config))

  var bytesWritten = 0L

  /**
    * Calculate (or estimate) the byte size of the outgoing message. Used internally
    * by HdfsWriters to decide when to cut a new output file based on max size.
    */
  def getOutputSizeInBytes(writable: Writable): Long = {
    0
  }

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
    if (shouldStartNewOutputFile) {
      close
      writer = getNextWriter
    }

    writer.map { seq =>
      val record = outgoing.getMessage.asInstanceOf[avroClass.type]
      //bytesWritten += getOutputSizeInBytes(record)
      seq.append(record)
    }
  }

  override def close: Unit = {
    writer.map { w => w.flush ; IOUtils.closeStream(w) }
    writer = None
    bytesWritten = 0L
  }

  protected def shouldStartNewOutputFile: Boolean = {
    bytesWritten >= batchSize || bucketer.get.shouldChangeBucket
  }

  protected def getNextWriter: Option[DataFileWriter[Object]] = {
    val path = bucketer.get.getNextWritePath(dfs)
    val fileWriter = new DataFileWriter[Object](datumWriter);
    Some(fileWriter.create(schema, dfs.create(path)))
  }

}
