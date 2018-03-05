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

import org.apache.samza.config.{YarnConfig, Config, ScalaMapConfig}

object HdfsConfig {

  // date format string for date-pathed output
  val DATE_PATH_FORMAT_STRING = "systems.%s.producer.hdfs.bucketer.date.path.format"
  val DATE_PATH_FORMAT_STRING_DEFAULT = "yyyy_MM_dd-HH"

  // HDFS output base dir
  val BASE_OUTPUT_DIR = "systems.%s.producer.hdfs.base.output.dir"
  val BASE_OUTPUT_DIR_DEFAULT = "/user/%s/%s"

  // how much data to write before splitting off a new partfile
  val WRITE_BATCH_SIZE_BYTES = "systems.%s.producer.hdfs.write.batch.size.bytes"
  val WRITE_BATCH_SIZE_BYTES_DEFAULT = (1024L * 1024L * 256L).toString

  // how much data to write before splitting off a new partfile
  val WRITE_BATCH_SIZE_RECORDS = "systems.%s.producer.hdfs.write.batch.size.records"
  val WRITE_BATCH_SIZE_RECORDS_DEFAULT = (256L * 1024L).toString

  // human-readable compression type name to be interpreted/handled by the HdfsWriter impl
  val COMPRESSION_TYPE = "systems.%s.producer.hdfs.compression.type"
  val COMPRESSION_TYPE_DEFAULT = "none"

  // fully qualified class name of the HdfsWriter impl for the named Producer system
  val HDFS_WRITER_CLASS_NAME ="systems.%s.producer.hdfs.writer.class"
  val HDFS_WRITER_CLASS_NAME_DEFAULT = "org.apache.samza.system.hdfs.writer.BinarySequenceFileHdfsWriter"

  // fully qualified class name of the Bucketer impl the HdfsWriter should use to generate HDFS paths and filenames
  val BUCKETER_CLASS = "systems.%s.producer.hdfs.bucketer.class"
  val BUCKETER_CLASS_DEFAULT = "org.apache.samza.system.hdfs.writer.JobNameDateTimeBucketer"

  // capacity of the hdfs consumer buffer - the blocking queue used for storing messages
  val CONSUMER_BUFFER_CAPACITY = "systems.%s.consumer.bufferCapacity"
  val CONSUMER_BUFFER_CAPACITY_DEFAULT = 10.toString

  // number of max retries for the hdfs consumer readers per partition
  val CONSUMER_NUM_MAX_RETRIES = "system.%s.consumer.numMaxRetries"
  val CONSUMER_NUM_MAX_RETRIES_DEFAULT = 10.toString

  // white list used by directory partitioner to filter out unwanted files in a hdfs directory
  val CONSUMER_PARTITIONER_WHITELIST = "systems.%s.partitioner.defaultPartitioner.whitelist"
  val CONSUMER_PARTITIONER_WHITELIST_DEFAULT = ".*"

  // black list used by directory partitioner to filter out unwanted files in a hdfs directory
  val CONSUMER_PARTITIONER_BLACKLIST = "systems.%s.partitioner.defaultPartitioner.blacklist"
  val CONSUMER_PARTITIONER_BLACKLIST_DEFAULT = ""

  // group pattern used by directory partitioner for advanced partitioning
  val CONSUMER_PARTITIONER_GROUP_PATTERN = "systems.%s.partitioner.defaultPartitioner.groupPattern"
  val CONSUMER_PARTITIONER_GROUP_PATTERN_DEFAULT = ""

  // type of the file reader (avro, plain, etc.)
  val FILE_READER_TYPE = "systems.%s.consumer.reader"
  val FILE_READER_TYPE_DEFAULT = "avro"

  // staging directory for storing partition description
  val STAGING_DIRECTORY = "systems.%s.stagingDirectory"
  val STAGING_DIRECTORY_DEFAULT = ""

  implicit def Config2Hdfs(config: Config) = new HdfsConfig(config)

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
    getOrElse(HdfsConfig.WRITE_BATCH_SIZE_BYTES format systemName, HdfsConfig.WRITE_BATCH_SIZE_BYTES_DEFAULT).toLong
  }

  /**
    * Split output files from all writer tasks based on # of bytes written to optimize
    * MapReduce utilization for Hadoop jobs that will process the data later.
    */
  def getWriteBatchSizeRecords(systemName: String): Long = {
    getOrElse(HdfsConfig.WRITE_BATCH_SIZE_RECORDS format systemName, HdfsConfig.WRITE_BATCH_SIZE_RECORDS_DEFAULT).toLong
  }

  /**
   * Simple, human-readable label for various compression options. HdfsWriter implementations
   * can choose how to handle these individually, or throw an exception. Example: "none", "gzip", ...
   */
  def getCompressionType(systemName: String): String = {
    getOrElse(HdfsConfig.COMPRESSION_TYPE format systemName, HdfsConfig.COMPRESSION_TYPE_DEFAULT)
  }

  /**
   * Get the capacity of the hdfs consumer buffer - the blocking queue used for storing messages
   */
  def getConsumerBufferCapacity(systemName: String): Int = {
    getOrElse(HdfsConfig.CONSUMER_BUFFER_CAPACITY format systemName, HdfsConfig.CONSUMER_BUFFER_CAPACITY_DEFAULT).toInt
  }

  /**
    * Get number of max retries for the hdfs consumer readers per partition
    */
  def getConsumerNumMaxRetries(systemName: String): Int = {
    getOrElse(HdfsConfig.CONSUMER_NUM_MAX_RETRIES format systemName, HdfsConfig.CONSUMER_NUM_MAX_RETRIES_DEFAULT).toInt
  }

  /**
   * White list used by directory partitioner to filter out unwanted files in a hdfs directory
   */
  def getPartitionerWhiteList(systemName: String): String = {
    getOrElse(HdfsConfig.CONSUMER_PARTITIONER_WHITELIST format systemName, HdfsConfig.CONSUMER_PARTITIONER_WHITELIST_DEFAULT)
  }

  /**
   * Black list used by directory partitioner to filter out unwanted files in a hdfs directory
   */
  def getPartitionerBlackList(systemName: String): String = {
    getOrElse(HdfsConfig.CONSUMER_PARTITIONER_BLACKLIST format systemName, HdfsConfig.CONSUMER_PARTITIONER_BLACKLIST_DEFAULT)
  }

  /**
   * Group pattern used by directory partitioner for advanced partitioning
   */
  def getPartitionerGroupPattern(systemName: String): String = {
    getOrElse(HdfsConfig.CONSUMER_PARTITIONER_GROUP_PATTERN format systemName, HdfsConfig.CONSUMER_PARTITIONER_GROUP_PATTERN_DEFAULT)
  }

  /**
   * Get the type of the file reader (avro, plain, etc.)
   */
  def getFileReaderType(systemName: String): String = {
    getOrElse(HdfsConfig.FILE_READER_TYPE format systemName, HdfsConfig.FILE_READER_TYPE_DEFAULT)
  }

  /**
   * Staging directory for storing partition description. If not set, will use the staging directory set
   * by yarn job.
   */
  def getStagingDirectory(systemName: String): String = {
    getOrElse(HdfsConfig.STAGING_DIRECTORY format systemName, getOrElse(YarnConfig.YARN_JOB_STAGING_DIRECTORY, HdfsConfig.STAGING_DIRECTORY_DEFAULT))
  }
}
