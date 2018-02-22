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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.hdfs.reader.TestAvroFileHdfsReader;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.util.concurrent.UncheckedExecutionException;


public class TestHdfsSystemConsumer {

  private static final String SYSTEM_NAME = "hdfs";
  private static final String FIELD_1 = "field1";
  private static final String FIELD_2 = "field2";
  private static final String WORKING_DIRECTORY = TestHdfsSystemConsumer.class.getResource("/integTest").getPath();
  private static final String AVRO_FILE_1 = WORKING_DIRECTORY + "/TestHdfsSystemConsumer-01.avro";
  private static final String AVRO_FILE_2 = WORKING_DIRECTORY + "/TestHdfsSystemConsumer-02.avro";
  private static final String AVRO_FILE_3 = WORKING_DIRECTORY + "/TestHdfsSystemConsumer-03.avro";
  private static final int NUM_FILES = 3;
  private static final int NUM_EVENTS = 100;

  private Config generateDefaultConfig() throws IOException {
    Map<String, String> properties = new HashMap<>();
    properties.put(String.format(HdfsConfig.CONSUMER_PARTITIONER_WHITELIST(), SYSTEM_NAME), ".*TestHdfsSystemConsumer.*avro");
    Path stagingDirectory = Files.createTempDirectory("staging");
    stagingDirectory.toFile().deleteOnExit();
    properties.put(String.format(HdfsConfig.STAGING_DIRECTORY(), SYSTEM_NAME), stagingDirectory.toString());
    return new MapConfig(properties);
  }

  private void generateAvroDataFiles() throws Exception {
    TestAvroFileHdfsReader.writeTestEventsToFile(AVRO_FILE_1, NUM_EVENTS);
    TestAvroFileHdfsReader.writeTestEventsToFile(AVRO_FILE_2, NUM_EVENTS);
    TestAvroFileHdfsReader.writeTestEventsToFile(AVRO_FILE_3, NUM_EVENTS);
  }

  /*
   * A simple end to end test that covers the workflow from system admin to
   * partitioner, system consumer, and so on, making sure the basic functionality
   * works as expected.
   */
  @Test
  public void testHdfsSystemConsumerE2E() throws Exception {
    Config config = generateDefaultConfig();
    HdfsSystemFactory systemFactory = new HdfsSystemFactory();

    // create admin and do partitioning
    HdfsSystemAdmin systemAdmin = systemFactory.getAdmin(SYSTEM_NAME, config);
    String streamName = WORKING_DIRECTORY;
    Set<String> streamNames = new HashSet<>();
    streamNames.add(streamName);
    generateAvroDataFiles();
    Map<String, SystemStreamMetadata> streamMetadataMap = systemAdmin.getSystemStreamMetadata(streamNames);
    SystemStreamMetadata systemStreamMetadata = streamMetadataMap.get(streamName);
    Assert.assertEquals(NUM_FILES, systemStreamMetadata.getSystemStreamPartitionMetadata().size());

    // create consumer and read from files
    HdfsSystemConsumer systemConsumer = systemFactory.getConsumer(SYSTEM_NAME, config, new NoOpMetricsRegistry());
    Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> metadataMap = systemStreamMetadata.getSystemStreamPartitionMetadata();
    Set<SystemStreamPartition> systemStreamPartitionSet = new HashSet<>();
    metadataMap.forEach((partition, metadata) -> {
      SystemStreamPartition ssp = new SystemStreamPartition(SYSTEM_NAME, streamName, partition);
      systemStreamPartitionSet.add(ssp);
      String offset = metadata.getOldestOffset();
      systemConsumer.register(ssp, offset);
    });
    systemConsumer.start();

    // verify events read from consumer
    int eventsReceived = 0;
    int totalEvents = (NUM_EVENTS + 1) * NUM_FILES; // one "End of Stream" event in the end
    int remainingRetries = 100;
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> overallResults = new HashMap<>();
    while (eventsReceived < totalEvents && remainingRetries > 0) {
      remainingRetries--;
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> result = systemConsumer.poll(systemStreamPartitionSet, 1000);
      for(SystemStreamPartition ssp : result.keySet()) {
        List<IncomingMessageEnvelope> messageEnvelopeList = result.get(ssp);
        overallResults.putIfAbsent(ssp, new ArrayList<>());
        overallResults.get(ssp).addAll(messageEnvelopeList);
        if (overallResults.get(ssp).size() >= NUM_EVENTS + 1) {
          systemStreamPartitionSet.remove(ssp);
        }
        eventsReceived += messageEnvelopeList.size();
      }
    }
    Assert.assertEquals("Did not receive all the events. Retry counter = " + remainingRetries, totalEvents, eventsReceived);
    Assert.assertEquals(NUM_FILES, overallResults.size());
    overallResults.values().forEach(messages -> {
      Assert.assertEquals(NUM_EVENTS + 1, messages.size());
      for (int index = 0;index < NUM_EVENTS; index++) {
        GenericRecord record = (GenericRecord) messages.get(index).getMessage();
        Assert.assertEquals(index % NUM_EVENTS, record.get(FIELD_1));
        Assert.assertEquals("string_" + (index % NUM_EVENTS), record.get(FIELD_2).toString());
      }
      Assert.assertEquals(messages.get(NUM_EVENTS).getOffset(), IncomingMessageEnvelope.END_OF_STREAM_OFFSET);
    });
  }

  /*
   * Ensure that empty staging directory will not break system admin,
   * but should fail system consumer
   */
  @Test
  public void testEmptyStagingDirectory() throws Exception {
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(HdfsConfig.CONSUMER_PARTITIONER_WHITELIST(), SYSTEM_NAME), ".*avro");
    Config config = new MapConfig(configMap);
    HdfsSystemFactory systemFactory = new HdfsSystemFactory();

    // create admin and do partitioning
    HdfsSystemAdmin systemAdmin = systemFactory.getAdmin(SYSTEM_NAME, config);
    String stream = WORKING_DIRECTORY;
    Set<String> streamNames = new HashSet<>();
    streamNames.add(stream);
    generateAvroDataFiles();
    Map<String, SystemStreamMetadata> streamMetadataMap = systemAdmin.getSystemStreamMetadata(streamNames);
    SystemStreamMetadata systemStreamMetadata = streamMetadataMap.get(stream);
    Assert.assertEquals(NUM_FILES, systemStreamMetadata.getSystemStreamPartitionMetadata().size());

    // create consumer and read from files
    HdfsSystemConsumer systemConsumer = systemFactory.getConsumer(SYSTEM_NAME, config, new NoOpMetricsRegistry());
    Partition partition = new Partition(0);
    SystemStreamPartition ssp = new SystemStreamPartition(SYSTEM_NAME, stream, partition);
    try {
      systemConsumer.register(ssp, "0");
      Assert.fail("Empty staging directory should fail system consumer");
    } catch (UncheckedExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof SamzaException);
    }
  }
}
