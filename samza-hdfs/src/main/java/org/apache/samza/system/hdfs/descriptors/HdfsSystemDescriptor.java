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
package org.apache.samza.system.hdfs.descriptors;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang3.StringUtils;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.system.hdfs.HdfsConfig;
import org.apache.samza.system.hdfs.HdfsSystemFactory;


/**
 * A {@link HdfsSystemDescriptor} can be used for specifying Samza and HDFS-specific properties of a HDFS
 * input/output system. It can also be used for obtaining {@link HdfsInputDescriptor}s and
 * {@link HdfsOutputDescriptor}s, which can be used for specifying Samza and system-specific properties of
 * HDFS input/output streams.
 * <p>
 * System properties provided in configuration override corresponding properties specified using a descriptor.
 */
public class HdfsSystemDescriptor extends SystemDescriptor<HdfsSystemDescriptor> {
  private static final String FACTORY_CLASS_NAME = HdfsSystemFactory.class.getName();

  private Optional<String> datePathFormat = Optional.empty();
  private Optional<String> outputBaseDir = Optional.empty();
  private Optional<Long> writeBatchSizeBytes = Optional.empty();
  private Optional<Long> writeBatchSizeRecords = Optional.empty();
  private Optional<String> writeCompressionType = Optional.empty();
  private Optional<String> writerClass = Optional.empty();

  private Optional<Long> consumerBufferCapacity = Optional.empty();
  private Optional<Long> consumerMaxRetries = Optional.empty();
  private Optional<String> consumerWhiteList = Optional.empty();
  private Optional<String> consumerBlackList = Optional.empty();
  private Optional<String> consumerGroupPattern = Optional.empty();
  private Optional<String> consumerReader = Optional.empty();
  private Optional<String> consumerStagingDirectory = Optional.empty();

  public HdfsSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, null, null);
  }

  /**
   * Gets an {@link HdfsInputDescriptor} for the input stream of this system.
   * <p>
   * The message in the stream has no key and the value type is determined by reader type.
   *
   * @param streamId id of the input stream
   * @return an {@link HdfsInputDescriptor} for the hdfs input stream
   */
  public HdfsInputDescriptor getInputDescriptor(String streamId) {
    return new HdfsInputDescriptor(streamId, this);
  }

  /**
   * Gets an {@link HdfsOutputDescriptor} for the output stream of this system.
   * <p>
   * The message in the stream has no key and the value type is determined by writer class.
   *
   * @param streamId id of the output stream
   * @return an {@link HdfsOutputDescriptor} for the hdfs output stream
   */
  public HdfsOutputDescriptor getOutputDescriptor(String streamId) {
    return new HdfsOutputDescriptor(streamId, this);
  }

  /**
   * In an HdfsWriter implementation that performs time-based output bucketing,
   * the user may configure a date format (suitable for inclusion in a file path)
   * using <code>SimpleDateFormat</code> formatting that the Bucketer implementation will
   * use to generate HDFS paths and filenames. The more granular this date format, the more
   * often a bucketing HdfsWriter will begin a new date-path bucket when creating the next output file.
   * @param datePathFormat date path format
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withDatePathFormat(String datePathFormat) {
    this.datePathFormat = Optional.ofNullable(StringUtils.stripToNull(datePathFormat));
    return this;
  }

  /**
   * The base output directory into which all HDFS output for this job will be written.
   * @param outputBaseDir output base directory
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withOutputBaseDir(String outputBaseDir) {
    this.outputBaseDir = Optional.ofNullable(StringUtils.stripToNull(outputBaseDir));
    return this;
  }

  /**
   * Split output files from all writer tasks based on # of bytes written to optimize
   * MapReduce utilization for Hadoop jobs that will process the data later.
   * @param writeBatchSizeBytes write batch size in bytes.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withWriteBatchSizeBytes(long writeBatchSizeBytes) {
    this.writeBatchSizeBytes = Optional.of(writeBatchSizeBytes);
    return this;
  }

  /**
   * Split output files from all writer tasks based on # of bytes written to optimize
   * MapReduce utilization for Hadoop jobs that will process the data later.
   * @param writeBatchSizeRecords write batch size in records.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withWriteBatchSizeRecords(long writeBatchSizeRecords) {
    this.writeBatchSizeRecords = Optional.of(writeBatchSizeRecords);
    return this;
  }

  /**
   * Simple, human-readable label for various compression options. HdfsWriter implementations
   * can choose how to handle these individually, or throw an exception. Example: "none", "gzip", ...
   * @param writeCompressionType compression type for writer.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withWriteCompressionType(String writeCompressionType) {
    this.writeCompressionType = Optional.ofNullable(StringUtils.stripToNull(writeCompressionType));
    return this;
  }

  /**
   * The fully-qualified class name of the HdfsWriter subclass that will write for this system.
   * @param writerClassName writer class name.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withWriterClassName(String writerClassName) {
    this.writerClass = Optional.ofNullable(StringUtils.stripToNull(writerClassName));
    return this;
  }

  /**
   * The capacity of the hdfs consumer buffer - the blocking queue used for storing messages.
   * @param bufferCapacity the buffer capacity for HDFS consumer.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withConsumerBufferCapacity(long bufferCapacity) {
    this.consumerBufferCapacity = Optional.of(bufferCapacity);
    return this;
  }

  /**
   * Number of max retries for the hdfs consumer readers per partition.
   * @param maxRetries number of max retires for HDFS consumer.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withConsumerNumMaxRetries(long maxRetries) {
    this.consumerMaxRetries = Optional.of(maxRetries);
    return this;
  }

  /**
   * White list used by directory partitioner to filter out unwanted files in a hdfs directory.
   * @param whiteList white list for HDFS consumer inputs.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withConsumerWhiteList(String whiteList) {
    this.consumerWhiteList = Optional.ofNullable(StringUtils.stripToNull(whiteList));
    return this;
  }

  /**
   * Black list used by directory partitioner to filter out unwanted files in a hdfs directory.
   * @param blackList black list for HDFS consumer inputs.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withConsumerBlackList(String blackList) {
    this.consumerBlackList = Optional.ofNullable(StringUtils.stripToNull(blackList));
    return this;
  }

  /**
   * Group pattern used by directory partitioner for advanced partitioning.
   * @param groupPattern group parttern for HDFS consumer inputs.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withConsumerGroupPattern(String groupPattern) {
    this.consumerGroupPattern = Optional.ofNullable(StringUtils.stripToNull(groupPattern));
    return this;
  }

  /**
   * The type of the file reader for consumer (avro, plain, etc.)
   * @param readerType reader type for HDFS consumer inputs.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withReaderType(String readerType) {
    this.consumerReader = Optional.ofNullable(StringUtils.stripToNull(readerType));
    return this;
  }

  /**
   * Staging directory for storing partition description. If not set, will use the staging directory set
   * by yarn job.
   * @param stagingDirectory staging directory for HDFS consumer inputs.
   * @return this system descriptor
   */
  public HdfsSystemDescriptor withStagingDirectory(String stagingDirectory) {
    this.consumerStagingDirectory = Optional.ofNullable(StringUtils.stripToNull(stagingDirectory));
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    Map<String, String> config = new HashMap<>(super.toConfig());
    String systemName = getSystemName();

    datePathFormat.ifPresent(
        val -> config.put(String.format(HdfsConfig.DATE_PATH_FORMAT_STRING(), systemName), val));
    outputBaseDir.ifPresent(val -> config.put(String.format(HdfsConfig.BASE_OUTPUT_DIR(), systemName), val));
    writeBatchSizeBytes.ifPresent(
        val -> config.put(String.format(HdfsConfig.WRITE_BATCH_SIZE_BYTES(), systemName), String.valueOf(val)));
    writeBatchSizeRecords.ifPresent(
        val -> config.put(String.format(HdfsConfig.WRITE_BATCH_SIZE_RECORDS(), systemName), String.valueOf(val)));
    writeCompressionType.ifPresent(
        val -> config.put(String.format(HdfsConfig.COMPRESSION_TYPE(), systemName), val));
    writerClass.ifPresent(val -> config.put(String.format(HdfsConfig.HDFS_WRITER_CLASS_NAME(), systemName), val));

    consumerBufferCapacity.ifPresent(
        val -> config.put(String.format(HdfsConfig.CONSUMER_BUFFER_CAPACITY(), systemName), String.valueOf(val)));
    consumerMaxRetries.ifPresent(
        val -> config.put(String.format(HdfsConfig.CONSUMER_NUM_MAX_RETRIES(), systemName), String.valueOf(val)));
    consumerWhiteList.ifPresent(
        val -> config.put(String.format(HdfsConfig.CONSUMER_PARTITIONER_WHITELIST(), systemName), val));
    consumerBlackList.ifPresent(
        val -> config.put(String.format(HdfsConfig.CONSUMER_PARTITIONER_BLACKLIST(), systemName), val));
    consumerGroupPattern.ifPresent(
        val -> config.put(String.format(HdfsConfig.CONSUMER_PARTITIONER_GROUP_PATTERN(), systemName), val));
    consumerReader.ifPresent(val -> config.put(String.format(HdfsConfig.FILE_READER_TYPE(), systemName), val));
    consumerStagingDirectory.ifPresent(
        val -> config.put(String.format(HdfsConfig.STAGING_DIRECTORY(), systemName), val));

    return config;
  }
}
