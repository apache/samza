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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.junit.Assert;
import org.junit.Test;


import static org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import static org.apache.samza.system.hdfs.partitioner.FileSystemAdapter.FileMetadata;


public class TestDirectoryPartitioner {

  class TestFileSystemAdapter implements FileSystemAdapter {
    private List<FileMetadata> expectedList;

    public TestFileSystemAdapter(List<FileMetadata> expectedList) {
      this.expectedList = expectedList;
    }

    public List<FileMetadata> getAllFiles(String streamName) {
      return expectedList;
    }
  }

  private void verifyPartitionDescriptor(String[] inputFiles, int[][] expectedPartitioning, int expectedNumPartition,
    Map<Partition, List<String>> actualPartitioning) {
    Assert.assertEquals(expectedNumPartition, actualPartitioning.size());
    Set<String> actualPartitioningPath = new HashSet<>();
    actualPartitioning.values().forEach(list -> actualPartitioningPath.add(String.join(",", list)));
    for (int i = 0; i < expectedNumPartition; i++) {
      int[] indexes = expectedPartitioning[i];
      List<String> files = new ArrayList<>();
      for (int j : indexes) {
        files.add(inputFiles[j]);
      }
      files.sort(Comparator.<String>naturalOrder());
      String expectedCombinedPath = String.join(",", files);
      Assert.assertTrue(actualPartitioningPath.contains(expectedCombinedPath));
    }
  }

  @Test
  public void testBasicWhiteListFiltering() {
    List<FileMetadata> testList = new ArrayList<>();
    int NUM_INPUT = 9;
    String[] inputFiles = {
      "part-001.avro",
      "part-002.avro",
      "part-003.avro",
      "delta-01.avro",
      "part-005.avro",
      "delta-03.avro",
      "part-004.avro",
      "delta-02.avro",
      "part-006.avro"};
    long[] fileLength = {150582, 138132, 214005, 205738, 158273, 982345, 313245, 234212, 413232};
    for (int i = 0; i < NUM_INPUT; i++) {
      testList.add(new FileMetadata(inputFiles[i], fileLength[i]));
    }
    String whiteList = "part-.*\\.avro";
    String blackList = "";
    String groupPattern = "";
    int EXPECTED_NUM_PARTITION = 6;
    int[][] EXPECTED_PARTITIONING = {{0}, {1}, {2}, {4}, {6}, {8}};

    DirectoryPartitioner directoryPartitioner =
      new DirectoryPartitioner(whiteList, blackList, groupPattern, new TestFileSystemAdapter(testList));
    Map<Partition, SystemStreamPartitionMetadata> metadataMap = directoryPartitioner.getPartitionMetadataMap("hdfs", null);
    Assert.assertEquals(EXPECTED_NUM_PARTITION, metadataMap.size());
    Map<Partition, List<String>> descriptorMap = directoryPartitioner.getPartitionDescriptor("hdfs");
    verifyPartitionDescriptor(inputFiles, EXPECTED_PARTITIONING, EXPECTED_NUM_PARTITION, descriptorMap);
  }

  @Test
  public void testBasicBlackListFiltering() {
    List<FileMetadata> testList = new ArrayList<>();
    int NUM_INPUT = 9;
    String[] inputFiles = {
      "part-001.avro",
      "part-002.avro",
      "part-003.avro",
      "delta-01.avro",
      "part-005.avro",
      "delta-03.avro",
      "part-004.avro",
      "delta-02.avro",
      "part-006.avro"};
    long[] fileLength = {150582, 138132, 214005, 205738, 158273, 982345, 313245, 234212, 413232};
    for (int i = 0; i < NUM_INPUT; i++) {
      testList.add(new FileMetadata(inputFiles[i], fileLength[i]));
    }
    String whiteList = ".*";
    String blackList = "delta-.*\\.avro";
    String groupPattern = "";
    int EXPECTED_NUM_PARTITION = 6;
    int[][] EXPECTED_PARTITIONING = {{0}, {1}, {2}, {4}, {6}, {8}};

    DirectoryPartitioner directoryPartitioner =
      new DirectoryPartitioner(whiteList, blackList, groupPattern, new TestFileSystemAdapter(testList));
    Map<Partition, SystemStreamPartitionMetadata> metadataMap = directoryPartitioner.getPartitionMetadataMap("hdfs", null);
    Assert.assertEquals(EXPECTED_NUM_PARTITION, metadataMap.size());
    Map<Partition, List<String>> descriporMap = directoryPartitioner.getPartitionDescriptor("hdfs");
    verifyPartitionDescriptor(inputFiles, EXPECTED_PARTITIONING, EXPECTED_NUM_PARTITION, descriporMap);
  }

  @Test
  public void testWhiteListBlackListFiltering() {
    List<FileMetadata> testList = new ArrayList<>();
    int NUM_INPUT = 9;
    String[] inputFiles = {
      "part-001.avro",
      "part-002.avro",
      "part-003.avro",
      "delta-01.avro",
      "part-005.avro",
      "delta-03.avro",
      "part-004.avro",
      "delta-02.avro",
      "part-006.avro"};
    long[] fileLength = {150582, 138132, 214005, 205738, 158273, 982345, 313245, 234212, 413232};
    for (int i = 0; i < NUM_INPUT; i++) {
      testList.add(new FileMetadata(inputFiles[i], fileLength[i]));
    }
    String whiteList = "part-.*\\.avro";
    String blackList = "part-002.avro";
    String groupPattern = "";
    int EXPECTED_NUM_PARTITION = 5;
    int[][] EXPECTED_PARTITIONING = {{0}, {2}, {4}, {6}, {8}};

    DirectoryPartitioner directoryPartitioner =
      new DirectoryPartitioner(whiteList, blackList, groupPattern, new TestFileSystemAdapter(testList));
    Map<Partition, SystemStreamPartitionMetadata> metadataMap = directoryPartitioner.getPartitionMetadataMap("hdfs", null);
    Assert.assertEquals(EXPECTED_NUM_PARTITION, metadataMap.size());
    Map<Partition, List<String>> descriporMap = directoryPartitioner.getPartitionDescriptor("hdfs");
    verifyPartitionDescriptor(inputFiles, EXPECTED_PARTITIONING, EXPECTED_NUM_PARTITION, descriporMap);
  }

  @Test
  public void testBasicGrouping() {
    List<FileMetadata> testList = new ArrayList<>();
    int NUM_INPUT = 9;
    String[] inputFiles = {
      "00_10-run_2016-08-15-13-04-part.0.150582.avro",
      "00_10-run_2016-08-15-13-04-part.1.138132.avro",
      "00_10-run_2016-08-15-13-04-part.2.214005.avro",
      "00_10-run_2016-08-15-13-05-part.0.205738.avro",
      "00_10-run_2016-08-15-13-05-part.1.158273.avro",
      "00_10-run_2016-08-15-13-05-part.2.982345.avro",
      "00_10-run_2016-08-15-13-06-part.0.313245.avro",
      "00_10-run_2016-08-15-13-06-part.1.234212.avro",
      "00_10-run_2016-08-15-13-06-part.2.413232.avro"};
    long[] fileLength = {150582, 138132, 214005, 205738, 158273, 982345, 313245, 234212, 413232};
    for (int i = 0; i < NUM_INPUT; i++) {
      testList.add(new FileMetadata(inputFiles[i], fileLength[i]));
    }

    String whiteList = ".*\\.avro";
    String blackList = "";
    String groupPattern = ".*part\\.[id]\\..*\\.avro"; // 00_10-run_2016-08-15-13-04-part.[id].138132.avro
    int EXPECTED_NUM_PARTITION = 3;
    int[][] EXPECTED_PARTITIONING = {
      {0, 3, 6}, // files from index 0, 3, 6 should be grouped into one partition
      {1, 4, 7}, // similar as above
      {2, 5, 8}};

    DirectoryPartitioner directoryPartitioner =
      new DirectoryPartitioner(whiteList, blackList, groupPattern, new TestFileSystemAdapter(testList));
    Map<Partition, SystemStreamPartitionMetadata> metadataMap = directoryPartitioner.getPartitionMetadataMap("hdfs", null);
    Assert.assertEquals(EXPECTED_NUM_PARTITION, metadataMap.size());
    Map<Partition, List<String>> descriporMap = directoryPartitioner.getPartitionDescriptor("hdfs");
    verifyPartitionDescriptor(inputFiles, EXPECTED_PARTITIONING, EXPECTED_NUM_PARTITION, descriporMap);
  }

  @Test
  public void testValidDirectoryUpdating() {
    // the update is valid when there are only new files being added to the directory
    // no changes on the old files
    List<FileMetadata> testList = new ArrayList<>();
    int NUM_INPUT = 6;
    String[] inputFiles = {
      "part-001.avro",
      "part-002.avro",
      "part-003.avro",
      "part-005.avro",
      "part-004.avro",
      "part-006.avro"};
    long[] fileLength = {150582, 138132, 214005, 205738, 158273, 982345};
    for (int i = 0; i < NUM_INPUT; i++) {
      testList.add(new FileMetadata(inputFiles[i], fileLength[i]));
    }
    String whiteList = ".*";
    String blackList = "";
    String groupPattern = "";
    int EXPECTED_NUM_PARTITION = 6;
    int[][] EXPECTED_PARTITIONING = {{0}, {1}, {2}, {3}, {4}, {5}};

    DirectoryPartitioner directoryPartitioner =
      new DirectoryPartitioner(whiteList, blackList, groupPattern, new TestFileSystemAdapter(testList));
    Map<Partition, SystemStreamPartitionMetadata> metadataMap = directoryPartitioner.getPartitionMetadataMap("hdfs", null);
    Assert.assertEquals(EXPECTED_NUM_PARTITION, metadataMap.size());
    Map<Partition, List<String>> descriporMap = directoryPartitioner.getPartitionDescriptor("hdfs");
    verifyPartitionDescriptor(inputFiles, EXPECTED_PARTITIONING, EXPECTED_NUM_PARTITION, descriporMap);

    NUM_INPUT = 7;
    String[] updatedInputFiles = {
      "part-001.avro",
      "part-002.avro",
      "part-003.avro",
      "part-005.avro",
      "part-004.avro",
      "part-007.avro", // add a new file to the directory
      "part-006.avro"};
    long[] updatedFileLength = {150582, 138132, 214005, 205738, 158273, 2513454, 982345};
    testList.clear();
    for (int i = 0; i < NUM_INPUT; i++) {
      testList.add(new FileMetadata(updatedInputFiles[i], updatedFileLength[i]));
    }
    directoryPartitioner =
      new DirectoryPartitioner(whiteList, blackList, groupPattern, new TestFileSystemAdapter(testList));
    metadataMap = directoryPartitioner.getPartitionMetadataMap("hdfs", descriporMap);
    Assert.assertEquals(EXPECTED_NUM_PARTITION, metadataMap.size()); // still expect only 6 partitions instead of 7
    Map<Partition, List<String>> updatedDescriptorMap = directoryPartitioner.getPartitionDescriptor("hdfs");
    verifyPartitionDescriptor(inputFiles, EXPECTED_PARTITIONING, EXPECTED_NUM_PARTITION, updatedDescriptorMap);
  }

  @Test
  public void testInvalidDirectoryUpdating() {
    // the update is invalid when at least one old file is removed
    List<FileMetadata> testList = new ArrayList<>();
    int NUM_INPUT = 6;
    String[] inputFiles = {
      "part-001.avro",
      "part-002.avro",
      "part-003.avro",
      "part-005.avro",
      "part-004.avro",
      "part-006.avro"};
    long[] fileLength = {150582, 138132, 214005, 205738, 158273, 982345};
    for (int i = 0; i < NUM_INPUT; i++) {
      testList.add(new FileMetadata(inputFiles[i], fileLength[i]));
    }
    String whiteList = ".*";
    String blackList = "";
    String groupPattern = "";
    int EXPECTED_NUM_PARTITION = 6;
    int[][] EXPECTED_PARTITIONING = {{0}, {1}, {2}, {3}, {4}, {5}};

    DirectoryPartitioner directoryPartitioner =
      new DirectoryPartitioner(whiteList, blackList, groupPattern, new TestFileSystemAdapter(testList));
    Map<Partition, SystemStreamPartitionMetadata> metadataMap = directoryPartitioner.getPartitionMetadataMap("hdfs", null);
    Assert.assertEquals(EXPECTED_NUM_PARTITION, metadataMap.size());
    Map<Partition, List<String>> descriporMap = directoryPartitioner.getPartitionDescriptor("hdfs");
    verifyPartitionDescriptor(inputFiles, EXPECTED_PARTITIONING, EXPECTED_NUM_PARTITION, descriporMap);

    String[] updatedInputFiles = {
      "part-001.avro",
      "part-002.avro",
      "part-003.avro",
      "part-005.avro",
      "part-007.avro", // remove part-004 and replace it with 007
      "part-006.avro"};
    long[] updatedFileLength = {150582, 138132, 214005, 205738, 158273, 982345};
    testList.clear();
    for (int i = 0; i < NUM_INPUT; i++) {
      testList.add(new FileMetadata(updatedInputFiles[i], updatedFileLength[i]));
    }
    directoryPartitioner =
      new DirectoryPartitioner(whiteList, blackList, groupPattern, new TestFileSystemAdapter(testList));
    try {
      directoryPartitioner.getPartitionMetadataMap("hdfs", descriporMap);
      Assert.fail("Expect exception thrown from getting metadata. Should not reach this point.");
    } catch (SamzaException e) {
      // expect exception to be thrown
    }
  }
}
