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

package org.apache.samza.system.hdfs


import java.text.SimpleDateFormat
import java.util.UUID

import org.apache.samza.SamzaException
import org.apache.samza.config.SystemConfig.Config2System
import org.apache.samza.config.{Config, ScalaMapConfig}
import org.apache.samza.util.{Logging, Util}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


object HdfsConfig {

  // date format string for date-pathed output
  val DATE_PATH_FORMAT_STRING = "systems.%s.producer.hdfs.bucketer.date.path.format"
  val DATE_PATH_FORMAT_STRING_DEFAULT = "yyyy_MM_dd-HH"

  // HDFS output base dir
  val BASE_OUTPUT_DIR = "systems.%s.producer.hdfs.base.output.dir"
  val BASE_OUTPUT_DIR_DEFAULT = "/user/%s/%s"

  // how much data to write before splitting off a new partfile
  val WRITE_BATCH_SIZE = "systems.%s.producer.hdfs.write.batch.size.bytes"
  val WRITE_BATCH_SIZE_DEFAULT = (1024L * 1024L * 256L).toString

  // human-readable compression type name to be interpreted/handled by the HdfsWriter impl
  val COMPRESSION_TYPE = "systems.%s.producer.hdfs.compression.type"
  val COMPRESSION_TYPE_DEFAULT = "none"

  // fully qualified class name of the HdfsWriter impl for the named Producer system
  val HDFS_WRITER_CLASS_NAME ="systems.%s.producer.hdfs.writer.class"
  val HDFS_WRITER_CLASS_NAME_DEFAULT = "org.apache.samza.system.hdfs.writer.BinarySequenceFileHdfsWriter"

  // fully qualified class name of the Bucketer impl the HdfsWriter should use to generate HDFS paths and filenames
  val BUCKETER_CLASS = "systems.%s.producer.hdfs.bucketer.class"
  val BUCKETER_CLASS_DEFAULT = "org.apache.samza.system.hdfs.writer.JobNameDateTimeBucketer"

  implicit def Hdfs2Kafka(config: Config) = new HdfsConfig(config)

}


class HdfsConfig(config: Config) extends ScalaMapConfig(config) {

  /**
   * @return the fully-qualified class name of the HdfsWriter subclass that will write for this system.
   */
  def getHdfsWriterClassName(systemName: String): String = {
    getOrElse(HdfsConfig.HDFS_WRITER_CLASS_NAME format systemName, HdfsConfig.HDFS_WRITER_CLASS_NAME_DEFAULT)
  }

  /**
   * The base output directory into which all HDFS output for this job will be written.
   */
  def getBaseOutputDir(systemName: String): String = {
    getOrElse(HdfsConfig.BASE_OUTPUT_DIR format systemName,
      HdfsConfig.BASE_OUTPUT_DIR_DEFAULT  format (System.getProperty("user.name"), systemName))
  }

  /**
   * The Bucketer subclass to instantiate for the job run.
   */
  def getHdfsBucketerClassName(systemName: String): String = {
    getOrElse(HdfsConfig.BUCKETER_CLASS format systemName, HdfsConfig.BUCKETER_CLASS_DEFAULT)
  }

  /**
   * In an HdfsWriter implementation that peforms time-based output bucketing,
   * the user may configure a date format (suitable for inclusion in a file path)
   * using <code>SimpleDateFormat</code> formatting that the Bucketer implementation will
   * use to generate HDFS paths and filenames. The more granular this date format, the more
   * often a bucketing HdfsWriter will begin a new date-path bucket when creating the next output file.
   */
  def getDatePathFormatter(systemName: String): SimpleDateFormat = {
    new SimpleDateFormat(getOrElse(HdfsConfig.DATE_PATH_FORMAT_STRING format systemName, HdfsConfig.DATE_PATH_FORMAT_STRING_DEFAULT))
  }

  def getFileUniqifier(systemName: String): String = {
    systemName + "-" + UUID.randomUUID + "-"
  }

  /**
   * Split output files from all writer tasks based on # of bytes written to optimize
   * MapReduce utilization for Hadoop jobs that will process the data later.
   */
  def getWriteBatchSizeBytes(systemName: String): Long = {
    getOrElse(HdfsConfig.WRITE_BATCH_SIZE format systemName, HdfsConfig.WRITE_BATCH_SIZE_DEFAULT).toLong
  }

  /**
   * Simple, human-readable label for various compression options. HdfsWriter implementations
   * can choose how to handle these individually, or throw an exception. Example: "none", "gzip", ...
   */
  def getCompressionType(systemName: String): String = {
    getOrElse(HdfsConfig.COMPRESSION_TYPE format systemName, HdfsConfig.COMPRESSION_TYPE_DEFAULT)
  }

}
