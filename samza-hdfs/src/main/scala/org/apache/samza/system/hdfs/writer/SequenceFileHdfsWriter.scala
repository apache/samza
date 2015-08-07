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


import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{SequenceFile, Writable, IOUtils}
import org.apache.hadoop.io.SequenceFile.Writer
import org.apache.hadoop.io.compress.{CompressionCodec, DefaultCodec, GzipCodec, SnappyCodec}

import org.apache.samza.system.hdfs.HdfsConfig
import org.apache.samza.system.hdfs.HdfsConfig._
import org.apache.samza.system.OutgoingMessageEnvelope


abstract class SequenceFileHdfsWriter(dfs: FileSystem, systemName: String, config: HdfsConfig)
  extends HdfsWriter[SequenceFile.Writer](dfs, systemName, config) {

  val batchSize = config.getWriteBatchSizeBytes(systemName)
  val bucketer = Some(Bucketer.getInstance(systemName, config))

  var bytesWritten = 0L

  /**
   * Generate a key (usually a singleton dummy value) appropriate for the SequenceFile you're writing
   */
  def getKey: Writable

  /**
   * Wrap the outgoing message in an appropriate Writable
   */
  def getValue(outgoing: OutgoingMessageEnvelope): Writable

  /**
   * Calculate (or estimate) the byte size of the outgoing message. Used internally
   * by HdfsWriters to decide when to cut a new output file based on max size.
   */
  def getOutputSizeInBytes(writable: Writable): Long

  /**
   * The Writable key class for the SequenceFile type
   */
  def keyClass: Class[_ <: Writable]

  /**
   * The Writable value class for the SequenceFile type
   */
  def valueClass: Class[_ <: Writable]

 /**
   *  Accepts a human-readable compression type from the job properties file such as
   *  "gzip", "snappy", or "none" - returns an appropriate SequenceFile CompressionCodec.
   */
  def getCompressionCodec(compressionType: String) = {
    compressionType match {
      case "snappy" => new SnappyCodec()

      case "gzip"   => new GzipCodec()

      case _        => new DefaultCodec()
    }
  }

  override def flush: Unit = writer.map { _.hflush }

  override def write(outgoing: OutgoingMessageEnvelope): Unit = {
    if (shouldStartNewOutputFile) {
      close 
      writer = getNextWriter
    }

    writer.map { seq =>
      val writable = getValue(outgoing)
      bytesWritten += getOutputSizeInBytes(writable)
      seq.append(getKey, writable)
    }
  }

  override def close: Unit = {
    writer.map { w => w.hflush ; IOUtils.closeStream(w) }
    writer = None
    bytesWritten = 0L
  }

  protected def shouldStartNewOutputFile: Boolean = {
    bytesWritten >= batchSize || bucketer.get.shouldChangeBucket
  }

  protected def getNextWriter: Option[SequenceFile.Writer] = {
    val path = bucketer.get.getNextWritePath(dfs)
    Some(
      SequenceFile.createWriter(
        dfs.getConf,
        Writer.stream(dfs.create(path)),
        Writer.keyClass(keyClass),
        Writer.valueClass(valueClass),
        Writer.compression(
          SequenceFile.CompressionType.BLOCK,
          getCompressionCodec(config.getCompressionType(systemName))
        )
      )
    )
  }

}
