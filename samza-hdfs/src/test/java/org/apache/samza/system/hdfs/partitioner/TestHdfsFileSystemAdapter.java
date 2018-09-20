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

import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamMetadata;
import org.junit.Assert;
import org.junit.Test;


public class TestHdfsFileSystemAdapter {

  @Test
  public void testGetAllFiles()
    throws Exception {
    URL url = this.getClass().getResource("/partitioner");
    FileSystemAdapter adapter = new HdfsFileSystemAdapter();
    List<FileSystemAdapter.FileMetadata> result =
      adapter.getAllFiles(url.getPath());
    Assert.assertEquals(3, result.size());
  }

  @Test
  public void testIntegrationWithPartitioner() throws Exception {
    URL url = this.getClass().getResource("/partitioner");
    String whiteList = ".*";
    String blackList = ".*02";
    String groupPattern = "";
    String streamName = String.format(url.getPath());
    DirectoryPartitioner directoryPartitioner =
      new DirectoryPartitioner(whiteList, blackList, groupPattern, new HdfsFileSystemAdapter());
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> metadataMap = directoryPartitioner.getPartitionMetadataMap(streamName, null);
    Assert.assertEquals(1, metadataMap.size());
    Map<Partition, List<String>> descriporMap = directoryPartitioner.getPartitionDescriptor(streamName);
    Assert.assertEquals(1, descriporMap.values().size());
    Assert.assertTrue(descriporMap.get(new Partition(0)).get(0).endsWith("testfile01"));
  }
}
