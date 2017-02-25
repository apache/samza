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

package org.apache.samza.system.hdfs.partitioner;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringUtils;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.system.hdfs.reader.MultiFileHdfsReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import static org.apache.samza.system.hdfs.partitioner.FileSystemAdapter.FileMetadata;


/**
 * The partitioner that takes a directory as an input and does
 * 1. Filtering, based on a white list and a black list
 * 2. Grouping, based on grouping pattern
 *
 * And then generate the partition metadata and partition descriptors
 *
 * This class holds the assumption that the directory remains immutable.
 * If the directory does changes:
 * ignore new files showing up in the directory based on an old version of partition descriptor;
 * throw {@link org.apache.samza.SamzaException} if at least one old file doesn't exist anymore
 */
public class DirectoryPartitioner {

  private static final Logger LOG = LoggerFactory.getLogger(DirectoryPartitioner.class);
  private static final String GROUP_IDENTIFIER = "\\[id]";

  private String whiteListRegex;
  private String blackListRegex;
  private String groupPattern;
  private FileSystemAdapter fileSystemAdapter;

  // stream name => partition => partition descriptor
  private Map<String, Map<Partition, List<String>>> partitionDescriptorMap = new HashMap<>();

  public DirectoryPartitioner(String whiteList, String blackList, String groupPattern,
    FileSystemAdapter fileSystemAdapter) {
    this.whiteListRegex = whiteList;
    this.blackListRegex = blackList;
    this.groupPattern = groupPattern;
    this.fileSystemAdapter = fileSystemAdapter;
    LOG.info(String
      .format("Creating DirectoryPartitioner with whiteList=%s, blackList=%s, groupPattern=%s", whiteList, blackList,
        groupPattern));
  }

  /*
   * Based on the stream name, get the list of all files and filter out unneeded ones given
   * the white list and black list
   */
  private List<FileMetadata> getFilteredFiles(String streamName) {
    List<FileMetadata> filteredFiles = new ArrayList<>();
    List<FileMetadata> allFiles = fileSystemAdapter.getAllFiles(streamName);
    LOG.info(String.format("List of all files for %s: %s", streamName, allFiles));
    allFiles.stream().filter(file -> file.getPath().matches(whiteListRegex) && !file.getPath().matches(blackListRegex))
      .forEach(filteredFiles::add);
    // sort the files to have a consistent order
    filteredFiles.sort((f1, f2) -> f1.getPath().compareTo(f2.getPath()));
    LOG.info(String.format("List of filtered files for %s: %s", streamName, filteredFiles));
    return filteredFiles;
  }

  /*
   * Algorithm to extract the group identifier from the path based on
   * the group pattern.
   * 1. Split the group pattern into two parts (prefix, suffix)
   * 2. Match the both prefix and suffix against the input
   * 3. Strip the prefix and suffix and then we can get the group identifier
   *
   * For example,
   * input = run_2016-08-01-part-3.avro
   * group pattern = ".*part-[id]/.avro"
   *
   * 1. Split: prefix pattern = ".*part-" suffix pattern = "/.avro"
   * 2. Match: prefix string = "run_2016-08-01-part-" suffix string = ".avro"
   * 3. Extract: output = "3"
   *
   * If we can't extract a group identifier, return the original input
   */
  private String extractGroupIdentifier(String input) {
    if (StringUtils.isBlank(GROUP_IDENTIFIER)) {
      return input;
    }
    String[] patterns = groupPattern.split(GROUP_IDENTIFIER);
    if (patterns.length != 2) {
      return input;
    }

    Pattern p1 = Pattern.compile(patterns[0]);
    Pattern p2 = Pattern.compile(patterns[1]);
    Matcher m1 = p1.matcher(input);
    Matcher m2 = p2.matcher(input);
    if (!m1.find()) {
      return input;
    }
    int s1 = m1.end();
    if (!m2.find(s1)) {
      return input;
    }
    int s2 = m2.start();
    return input.substring(s1, s2);
  }

  /*
   * Group partitions based on the group identifier extracted from the file path
   */
  private List<List<FileMetadata>> generatePartitionGroups(List<FileMetadata> filteredFiles) {
    Map<String, List<FileMetadata>> map = new HashMap<>();
    for (FileMetadata fileMetadata : filteredFiles) {
      String groupId = extractGroupIdentifier(fileMetadata.getPath());
      map.putIfAbsent(groupId, new ArrayList<>());
      map.get(groupId).add(fileMetadata);
    }
    List<List<FileMetadata>> ret = new ArrayList<>();
    // sort the map to guarantee consistent ordering
    List<String> sortedKeys = new ArrayList<>(map.keySet());
    sortedKeys.sort(Comparator.<String>naturalOrder());
    sortedKeys.stream().forEach(key -> ret.add(map.get(key)));
    return ret;
  }

   /*
    * This class holds the assumption that the directory remains immutable.
    * If the directory does changes:
    * ignore new files showing up in the directory based on an old version of partition descriptor;
    * throw {@link org.apache.samza.SamzaException} if at least one old file doesn't exist anymore
    */
  private List<FileMetadata> validateAndGetOriginalFilteredFiles(List<FileMetadata> newFileList,
    Map<Partition, List<String>> existingPartitionDescriptor) {
    assert newFileList != null;
    assert existingPartitionDescriptor != null;
    Set<String> oldFileSet = new HashSet<>();
    existingPartitionDescriptor.values().forEach(oldFileSet::addAll);
    Set<String> newFileSet = new HashSet<>();
    newFileList.forEach(file -> newFileSet.add(file.getPath()));
    if (!newFileSet.containsAll(oldFileSet)) {
      throw new SamzaException("The list of new files is not a super set of the old files. diff = "
        + oldFileSet.removeAll(newFileSet));
    }
    Iterator<FileMetadata> iterator = newFileList.iterator();
    while (iterator.hasNext()) {
      FileMetadata file = iterator.next();
      if (!oldFileSet.contains(file.getPath())) {
        iterator.remove();
      }
    }
    return newFileList;
  }

  /**
   * Get partition metadata for a stream
   * @param streamName name of the stream; should contain the information about the path of the
   *                   root directory
   * @param existingPartitionDescriptorMap map of the existing partition descriptor
   * @return map of SSP metadata
   */
  public Map<Partition, SystemStreamPartitionMetadata> getPartitionMetadataMap(String streamName,
    @Nullable Map<Partition, List<String>> existingPartitionDescriptorMap) {
    LOG.info("Trying to obtain metadata for " + streamName);
    LOG.info("Existing partition descriptor: " + (existingPartitionDescriptorMap == null ? "empty"
      : existingPartitionDescriptorMap));
    Map<Partition, SystemStreamPartitionMetadata> partitionMetadataMap = new HashMap<>();
    partitionDescriptorMap.putIfAbsent(streamName, new HashMap<>());
    List<FileMetadata> filteredFiles = getFilteredFiles(streamName);
    if (existingPartitionDescriptorMap != null) {
      filteredFiles = validateAndGetOriginalFilteredFiles(filteredFiles, existingPartitionDescriptorMap);
    }
    List<List<FileMetadata>> groupedPartitions = generatePartitionGroups(filteredFiles);
    int partitionId = 0;
    for (List<FileMetadata> fileGroup : groupedPartitions) {
      Partition partition = new Partition(partitionId);
      List<String> pathList = new ArrayList<>();
      List<String> lengthList = new ArrayList<>();
      fileGroup.forEach(fileMetadata -> {
        pathList.add(fileMetadata.getPath());
        lengthList.add(String.valueOf(fileMetadata.getLen()));
      });
      String oldestOffset = MultiFileHdfsReader.generateOffset(0, "0");
      String newestOffset = MultiFileHdfsReader.generateOffset(lengthList.size() - 1, String.valueOf(lengthList.get(lengthList.size() - 1)));
      SystemStreamPartitionMetadata metadata =
        new SystemStreamPartitionMetadata(oldestOffset, newestOffset, null);
      partitionMetadataMap.put(partition, metadata);
      partitionDescriptorMap.get(streamName).put(partition, pathList);
      partitionId++;
    }
    LOG.info("Obtained metadata map as: " + partitionMetadataMap);
    LOG.info("Computed partition description as: " + partitionDescriptorMap);
    return partitionMetadataMap;
  }

  /**
   * Get partition descriptors for a stream
   * @param streamName name of the stream; should contain the information about the path of the
   *                   root directory
   * @return map of the partition descriptor
   */
  public Map<Partition, List<String>> getPartitionDescriptor(String streamName) {
    return partitionDescriptorMap.get(streamName);
  }
}
