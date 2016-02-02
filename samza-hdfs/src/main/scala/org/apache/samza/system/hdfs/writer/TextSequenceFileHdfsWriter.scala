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
import org.apache.hadoop.io.{SequenceFile, Writable, LongWritable, Text}

import org.apache.samza.system.hdfs.HdfsConfig
import org.apache.samza.system.hdfs.HdfsConfig._
import org.apache.samza.system.OutgoingMessageEnvelope


/**
 * Implentation of HdfsWriter for SequenceFiles using LongWritable keys and BytesWritable values.
 * The key type is currently just a dummy record. This class is usable when the outgoing message
 * can be converted directly to a String at write time.
 */
class TextSequenceFileHdfsWriter(dfs: FileSystem, systemName: String, config: HdfsConfig) extends SequenceFileHdfsWriter(dfs, systemName, config) {
  private lazy val defaultLongWritableKey = new LongWritable(0L)

  def getKey = defaultLongWritableKey

  def getValue(outgoing: OutgoingMessageEnvelope) = {
    new Text(outgoing.getMessage.asInstanceOf[String])
  }

  def getOutputSizeInBytes(writable: Writable) = {
    writable.asInstanceOf[Text].getLength
  }

  def keyClass = classOf[LongWritable]

  def valueClass = classOf[Text]

}
