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

package org.apache.samza.system.hdfs.reader;

import java.util.List;

import org.apache.samza.SamzaException;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.hdfs.HdfsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A wrapper on top of {@link org.apache.samza.system.hdfs.reader.SingleFileHdfsReader}
 * to manage the situation of multiple files per partition.
 *
 * The offset for MultiFileHdfsReader, which is also the offset that gets
 * committed in and used by Samza, consists of two parts: file index,
 * actual offset within file. For example, 3:127
 *
 * Format of the offset within file is defined by the implementation of
 * {@link org.apache.samza.system.hdfs.reader.SingleFileHdfsReader} itself.
 */
public class MultiFileHdfsReader {
  private static final Logger LOG = LoggerFactory.getLogger(MultiFileHdfsReader.class);
  private static final String DELIMITER = ":";

  private final HdfsReaderFactory.ReaderType readerType;
  private final SystemStreamPartition systemStreamPartition;
  private List<String> filePaths;
  private SingleFileHdfsReader curReader;
  private int curFileIndex = 0;
  private String curSingleFileOffset;
  private int numRetries;
  private int numMaxRetries;

  /**
   * Get the current file index from the offset string
   * @param offset offset string that contains both file index and offset within file
   * @return the file index part
   */
  public static int getCurFileIndex(String offset) {
    if (offset == null){
      return 0;
    }
    String[] elements = offset.split(DELIMITER);
    if (elements.length < 2) {
      throw new SamzaException("Invalid offset for MultiFileHdfsReader: " + offset);
    }
    return Integer.parseInt(elements[0]);
  }

  /**
   * Get the offset within file from the offset string
   * @param offset offset string that contains both file index and offset within file
   * @return the single file offset part
   */
  public static String getCurSingleFileOffset(String offset) {
    if (offset == null){
      return "0:0";
    }
    String[] elements = offset.split(DELIMITER);
    if (elements.length < 2) {
      throw new SamzaException("Invalid offset for MultiFileHdfsReader: " + offset);
    }
    // Getting the remaining of the offset string in case the single file
    // offset uses the same delimiter.
    return offset.substring(elements[0].length() + 1);
  }

  /**
   * Generate the offset based on file index and offset within single file
   * @param fileIndex index of the file
   * @param singleFileOffset offset within single file
   * @return the complete offset
   */
  public static String generateOffset(int fileIndex, String singleFileOffset) {
    return fileIndex + DELIMITER + singleFileOffset;
  }

  /*
   * Get current offset: offset of the LAST message being successfully read. If no messages have
   * ever been read, return the offset of first event.
   */
  private String getCurOffset() {
    return generateOffset(curFileIndex, curSingleFileOffset);
  }

  public MultiFileHdfsReader(HdfsReaderFactory.ReaderType readerType, SystemStreamPartition systemStreamPartition,
    List<String> partitionDescriptors, String offset) {
    this(readerType, systemStreamPartition, partitionDescriptors, offset,
      Integer.parseInt(HdfsConfig.CONSUMER_NUM_MAX_RETRIES_DEFAULT()));
  }

  private void init(String offset) {
    if (curReader != null) {
      curReader.close();
      curReader = null;
    }
    curFileIndex = getCurFileIndex(offset);
    if (curFileIndex >= filePaths.size()) {
      throw new SamzaException(
        String.format("Invalid file index %d. Number of files is %d", curFileIndex, filePaths.size()));
    }
    curSingleFileOffset = getCurSingleFileOffset(offset);
    curReader = HdfsReaderFactory.getHdfsReader(readerType, systemStreamPartition);
    curReader.open(filePaths.get(curFileIndex), curSingleFileOffset);
  }

  public MultiFileHdfsReader(HdfsReaderFactory.ReaderType readerType, SystemStreamPartition systemStreamPartition,
    List<String> partitionDescriptors, String offset, int numMaxRetries) {
    this.readerType = readerType;
    this.systemStreamPartition = systemStreamPartition;
    this.filePaths = partitionDescriptors;
    this.numMaxRetries = numMaxRetries;
    this.numRetries = 0;
    if (partitionDescriptors.size() <= 0) {
      throw new SamzaException(
        "Invalid number of files based on partition descriptors: " + partitionDescriptors.size());
    }
    init(offset);
  }

  public boolean hasNext() {
    while (curFileIndex < filePaths.size()) {
      if (curReader.hasNext()) {
        return true;
      }
      curReader.close();
      curFileIndex++;
      if (curFileIndex < filePaths.size()) {
        curReader = HdfsReaderFactory.getHdfsReader(readerType, systemStreamPartition);
        curReader.open(filePaths.get(curFileIndex), "0");
      }
    }
    return false;
  }

  public IncomingMessageEnvelope readNext() {
    if (!hasNext()) {
      LOG.warn("Attempting to read more data when there aren't any. ssp=" + systemStreamPartition);
      return null;
    }
    // record the next offset before we read, so when the read fails and we reconnect,
    // we seek to the same offset that we try below
    curSingleFileOffset = curReader.nextOffset();
    IncomingMessageEnvelope messageEnvelope = curReader.readNext();
    // Copy everything except for the offset. Turn the single-file style offset into a multi-file one
    return new IncomingMessageEnvelope(messageEnvelope.getSystemStreamPartition(), getCurOffset(),
      messageEnvelope.getKey(), messageEnvelope.getMessage(), messageEnvelope.getSize());
  }

  /**
   * Reconnect to the file systems in case of failure.
   * Reset offset to the last checkpoint (last successfully read message).
   * Throw {@link org.apache.samza.SamzaException} if reaches max number of
   * retries.
   */
  public void reconnect() {
    reconnect(getCurOffset());
  }

  /**
   * Reconnect to the file systems in case of failures.
   * @param offset reset offset to the specified offset
   * Throw {@link org.apache.samza.SamzaException} if reaches max number of
   * retries.
   */
  public void reconnect(String offset) {
    if (numRetries >= numMaxRetries) {
      throw new SamzaException(
        String.format("Give up reconnecting. numRetries: %d; numMaxRetries: %d", numRetries, numMaxRetries));
    }
    LOG.info(String
      .format("Reconnecting with offset: %s numRetries: %d numMaxRetries: %d", offset, numRetries, numMaxRetries));
    numRetries++;
    init(offset);
  }

  public void close() {
    LOG.info(String.format("MiltiFileHdfsReader shutdown requested for %s. Current offset = %s", systemStreamPartition,
      getCurOffset()));
    if (curReader != null) {
      curReader.close();
    }
  }

  public SystemStreamPartition getSystemStreamPartition() {
    return systemStreamPartition;
  }
}
