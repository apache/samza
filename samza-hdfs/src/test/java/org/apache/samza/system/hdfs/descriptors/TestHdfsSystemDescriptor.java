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

import java.util.Map;

import org.apache.samza.system.hdfs.HdfsConfig;
import org.apache.samza.system.hdfs.HdfsSystemFactory;
import org.apache.samza.system.hdfs.writer.AvroDataFileHdfsWriter;
import org.junit.Assert;
import org.junit.Test;


public class TestHdfsSystemDescriptor {
  @Test
  public void testMajorConfigGeneration() {
    String systemName = "hdfs";

    HdfsSystemDescriptor sd = new HdfsSystemDescriptor(systemName).withConsumerBufferCapacity(950)
        .withConsumerWhiteList(".*")
        .withReaderType("avro")
        .withOutputBaseDir("/home/output")
        .withWriterClassName(AvroDataFileHdfsWriter.class.getName());
    sd.getInputDescriptor("input");

    Map<String, String> generatedConfig = sd.toConfig();
    Assert.assertEquals(6, generatedConfig.size());
    System.out.println(generatedConfig);

    Assert.assertEquals(HdfsSystemFactory.class.getName(), generatedConfig.get("systems.hdfs.samza.factory"));
    Assert.assertEquals("950", generatedConfig.get(String.format(HdfsConfig.CONSUMER_BUFFER_CAPACITY(), systemName)));
    Assert.assertEquals(".*",
        generatedConfig.get(String.format(HdfsConfig.CONSUMER_PARTITIONER_WHITELIST(), systemName)));
    Assert.assertEquals("avro", generatedConfig.get(String.format(HdfsConfig.FILE_READER_TYPE(), systemName)));
    Assert.assertEquals("/home/output", generatedConfig.get(String.format(HdfsConfig.BASE_OUTPUT_DIR(), systemName)));
    Assert.assertEquals(AvroDataFileHdfsWriter.class.getName(),
        generatedConfig.get(String.format(HdfsConfig.HDFS_WRITER_CLASS_NAME(), systemName)));
  }
}
