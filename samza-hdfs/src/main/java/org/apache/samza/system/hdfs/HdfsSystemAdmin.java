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

package org.apache.samza.system.hdfs;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.hdfs.partitioner.DirectoryPartitioner;
import org.apache.samza.system.hdfs.partitioner.HdfsFileSystemAdapter;
import org.apache.samza.system.hdfs.reader.HdfsReaderFactory;
import org.apache.samza.system.hdfs.reader.MultiFileHdfsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The HDFS system admin for {@link org.apache.samza.system.hdfs.HdfsSystemConsumer} and
 * {@link org.apache.samza.system.hdfs.HdfsSystemProducer}
 *
 * A high level overview of the HDFS producer/consumer architecture:
 *                   ┌──────────────────────────────────────────────────────────────────────────────┐
 *                   │                                                                              │
 * ┌─────────────────┤                                     HDFS                                     │
 * │   Obtain        │                                                                              │
 * │  Partition      └──────┬──────────────────────▲──────┬─────────────────────────────────▲───────┘
 * │ Descriptors            │                      │      │                                 │
 * │                        │                      │      │                                 │
 * │          ┌─────────────▼───────┐              │      │       Filtering/                │
 * │          │                     │              │      └───┐    Grouping                 └─────┐
 * │          │ HDFSAvroFileReader  │              │          │                                   │
 * │          │                     │    Persist   │          │                                   │
 * │          └─────────┬───────────┘   Partition  │          │                                   │
 * │                    │              Descriptors │   ┌──────▼──────────────┐         ┌──────────┴──────────┐
 * │                    │                          │   │                     │         │                     │
 * │          ┌─────────┴───────────┐              │   │Directory Partitioner│         │   HDFSAvroWriter    │
 * │          │     IFileReader     │              │   │                     │         │                     │
 * │          │                     │              │   └──────┬──────────────┘         └──────────┬──────────┘
 * │          └─────────┬───────────┘              │          │                                   │
 * │                    │                          │          │                                   │
 * │                    │                          │          │                                   │
 * │          ┌─────────┴───────────┐            ┌─┴──────────┴────────┐               ┌──────────┴──────────┐
 * │          │                     │            │                     │               │                     │
 * │          │ HDFSSystemConsumer  │            │   HDFSSystemAdmin   │               │ HDFSSystemProducer  │
 * └──────────▶                     │            │                     │               │                     │
 *            └─────────┬───────────┘            └───────────┬─────────┘               └──────────┬──────────┘
 *                      │                                    │                                    │
 *                      └────────────────────────────────────┼────────────────────────────────────┘
 *                                                           │
 *                   ┌───────────────────────────────────────┴──────────────────────────────────────┐
 *                   │                                                                              │
 *                   │                              HDFSSystemFactory                               │
 *                   │                                                                              │
 *                   └──────────────────────────────────────────────────────────────────────────────┘
 */
public class HdfsSystemAdmin implements SystemAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsSystemAdmin.class);

  private HdfsConfig hdfsConfig;
  private DirectoryPartitioner directoryPartitioner;
  private String stagingDirectory; // directory that contains the partition description
  private HdfsReaderFactory.ReaderType readerType;

  public HdfsSystemAdmin(String systemName, Config config) {
    hdfsConfig = new HdfsConfig(config);
    directoryPartitioner = new DirectoryPartitioner(hdfsConfig.getPartitionerWhiteList(systemName),
      hdfsConfig.getPartitionerBlackList(systemName), hdfsConfig.getPartitionerGroupPattern(systemName),
      new HdfsFileSystemAdapter());
    stagingDirectory = hdfsConfig.getStagingDirectory(systemName);
    readerType = HdfsReaderFactory.getType(hdfsConfig.getFileReaderType(systemName));
  }

  @Override
  public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
    /*
     * To actually get the "after" offset we have to seek to that specific offset in the file and
     * read that record to figure out the location of next record. This is much more expensive operation
     * compared to the case in KafkaSystemAdmin.
     * So simply return the same offsets. This will always incur re-processing but such semantics are legit
     * in Samza.
     */
    return offsets;
  }

  static Map<Partition, List<String>> obtainPartitionDescriptorMap(String stagingDirectory, String streamName) {
    if (StringUtils.isBlank(stagingDirectory)) {
      LOG.info("Empty or null staging directory: {}", stagingDirectory);
      return Collections.emptyMap();
    }
    if (StringUtils.isBlank(streamName)) {
      throw new SamzaException(String.format("stream name (%s) is null or empty!", streamName));
    }
    Path path = PartitionDescriptorUtil.getPartitionDescriptorPath(stagingDirectory, streamName);
    try (FileSystem fs = path.getFileSystem(new Configuration())) {
      if (!fs.exists(path)) {
        return Collections.emptyMap();
      }
      try (FSDataInputStream fis = fs.open(path)) {
        String json = IOUtils.toString(fis, StandardCharsets.UTF_8);
        return PartitionDescriptorUtil.getDescriptorMapFromJson(json);
      }
    } catch (IOException e) {
      throw new SamzaException("Failed to read partition description from: " + path);
    }
  }

  /*
   * Persist the partition descriptor only when it doesn't exist already on HDFS.
   */
  private void persistPartitionDescriptor(String streamName,
    Map<Partition, List<String>> partitionDescriptorMap) {
    if (StringUtils.isBlank(stagingDirectory) || StringUtils.isBlank(streamName)) {
      LOG.warn("Staging directory ({}) or stream name ({}) is empty", stagingDirectory, streamName);
      return;
    }
    Path targetPath = PartitionDescriptorUtil.getPartitionDescriptorPath(stagingDirectory, streamName);
    try (FileSystem fs = targetPath.getFileSystem(new Configuration())) {
      // Partition descriptor is supposed to be immutable. So don't override it if it exists.
      if (fs.exists(targetPath)) {
        LOG.warn(targetPath.toString() + " exists. Skip persisting partition descriptor.");
      } else {
        LOG.info("About to persist partition descriptors to path: " + targetPath.toString());
        try (FSDataOutputStream fos = fs.create(targetPath)) {
          fos.write(
            PartitionDescriptorUtil.getJsonFromDescriptorMap(partitionDescriptorMap).getBytes(StandardCharsets.UTF_8));
        }
      }
    } catch (IOException e) {
      throw new SamzaException("Failed to validate/persist partition description on hdfs.", e);
    }
  }

  private boolean partitionDescriptorExists(String streamName) {
    if (StringUtils.isBlank(stagingDirectory) || StringUtils.isBlank(streamName)) {
      LOG.warn("Staging directory ({}) or stream name ({}) is empty", stagingDirectory, streamName);
      return false;
    }
    Path targetPath = PartitionDescriptorUtil.getPartitionDescriptorPath(stagingDirectory, streamName);
    try (FileSystem fs = targetPath.getFileSystem(new Configuration())) {
      return fs.exists(targetPath);
    } catch (IOException e) {
      throw new SamzaException("Failed to obtain information about path: " + targetPath);
    }
  }

  /**
   *
   * Fetch metadata from hdfs system for a set of streams. This has the potential side effect
   * to persist partition description to the staging directory on hdfs if staging directory
   * is not empty. See getStagingDirectory on {@link HdfsConfig}
   *
   * @param streamNames
   *          The streams to to fetch metadata for.
   * @return A map from stream name to SystemStreamMetadata for each stream
   *         requested in the parameter set.
   */
  @Override
  public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
    Map<String, SystemStreamMetadata> systemStreamMetadataMap = new HashMap<>();
    streamNames.forEach(streamName -> {
      systemStreamMetadataMap.put(streamName, new SystemStreamMetadata(streamName, directoryPartitioner
        .getPartitionMetadataMap(streamName, obtainPartitionDescriptorMap(stagingDirectory, streamName))));
      if (!partitionDescriptorExists(streamName)) {
        persistPartitionDescriptor(streamName, directoryPartitioner.getPartitionDescriptor(streamName));
      }
    });
    return systemStreamMetadataMap;
  }

  /**
   * Compare two multi-file style offset. A multi-file style offset consist of both
   * the file index as well as the offset within that file. And the format of it is:
   * "fileIndex:offsetWithinFile"
   * For example, "2:0", "3:127"
   * Format of the offset within file is defined by the implementation of
   * {@link org.apache.samza.system.hdfs.reader.SingleFileHdfsReader} itself.
   *
   * @param offset1 First offset for comparison.
   * @param offset2 Second offset for comparison.
   * @return -1, if offset1 @lt offset2
   *          0, if offset1 == offset2
   *          1, if offset1 @gt offset2
   *          null, if not comparable
   */
  @Override
  public Integer offsetComparator(String offset1, String offset2) {
    if (StringUtils.isBlank(offset1) || StringUtils.isBlank(offset2)) {
      return null;
    }
    int fileIndex1 = MultiFileHdfsReader.getCurFileIndex(offset1);
    int fileIndex2 = MultiFileHdfsReader.getCurFileIndex(offset2);
    if (fileIndex1 == fileIndex2) {
      String offsetWithinFile1 = MultiFileHdfsReader.getCurSingleFileOffset(offset1);
      String offsetWithinFile2 = MultiFileHdfsReader.getCurSingleFileOffset(offset2);
      return HdfsReaderFactory.offsetComparator(readerType, offsetWithinFile1, offsetWithinFile2);
    }
    return Integer.compare(fileIndex1, fileIndex2);
  }
}
