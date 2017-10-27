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

package org.apache.samza.system.eventhub.producer;

import com.microsoft.azure.eventhubs.*;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.Interceptor;
import org.apache.samza.system.eventhub.MockEventHubClientManagerFactory;
import org.apache.samza.system.eventhub.admin.PassThroughInterceptor;
import org.apache.samza.system.eventhub.producer.EventHubSystemProducer.PartitioningMethod;
import org.apache.samza.system.eventhub.TestMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.samza.system.eventhub.MockEventHubConfigFactory.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EventHubRuntimeInformation.class, EventHubClient.class, PartitionReceiver.class, PartitionSender.class})
public class TestEventHubSystemProducer {

  private static List<String> generateMessages(int numMsg) {
    Random rand = new Random(System.currentTimeMillis());
    List<String> messages = new ArrayList<>();
    for (int i = 0; i < numMsg; i++) {
      messages.add("message payload: " + rand.nextInt());
    }
    return messages;
  }

  @Test
  public void testSendingToSpecificPartitions() throws Exception {
    String systemName = "eventhubs";
    String streamName = "testStream";
    int numEvents = 10;
    int partitionId0 = 0;
    int partitionId1 = 1;

    TestMetricsRegistry testMetrics = new TestMetricsRegistry();
    Map<String, Interceptor> interceptor = new HashMap<>();
    interceptor.put(streamName, new PassThroughInterceptor());

    List<String> outgoingMessagesP0 = generateMessages(numEvents);
    List<String> outgoingMessagesP1 = generateMessages(numEvents);

    // Set configs
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), streamName);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, systemName, streamName), EVENTHUB_NAMESPACE);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, systemName, streamName), EVENTHUB_KEY_NAME);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, systemName, streamName), EVENTHUB_KEY);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName), EVENTHUB_ENTITY1);
    configMap.put(String.format(EventHubConfig.CONFIG_PRODUCER_PARTITION_METHOD, systemName),
            PartitioningMethod.PARTITION_KEY_AS_PARTITION.toString());

    MockEventHubClientManagerFactory factory = new MockEventHubClientManagerFactory();

    EventHubSystemProducer producer =
            new EventHubSystemProducer(new EventHubConfig(configMap), systemName, factory, interceptor, testMetrics);

    SystemStream systemStream = new SystemStream(systemName, streamName);
    producer.register(streamName);
    producer.start();

    outgoingMessagesP0.forEach(message ->
            producer.send(streamName, new OutgoingMessageEnvelope(systemStream, partitionId0, null, message.getBytes())));
    outgoingMessagesP1.forEach(message ->
            producer.send(streamName, new OutgoingMessageEnvelope(systemStream, partitionId1, null, message.getBytes())));

    // Retrieve sent data
    List<String> receivedData0 = factory.getSentData(systemName, streamName, partitionId0)
            .stream().map(eventData -> new String(eventData.getBytes())).collect(Collectors.toList());
    List<String> receivedData1 = factory.getSentData(systemName, streamName, partitionId1)
            .stream().map(eventData -> new String(eventData.getBytes())).collect(Collectors.toList());

    Assert.assertTrue(outgoingMessagesP0.equals(receivedData0));
    Assert.assertTrue(outgoingMessagesP1.equals(receivedData1));
  }

  @Test
  public void testSendingToEventHubHashing() throws Exception {
    String systemName = "eventhubs";
    String streamName = "testStream";
    int numEvents = 10;
    String partitionId0 = "124";
    String partitionId1 = "235";

    TestMetricsRegistry testMetrics = new TestMetricsRegistry();
    Map<String, Interceptor> interceptor = new HashMap<>();
    interceptor.put(streamName, new PassThroughInterceptor());

    List<String> outgoingMessagesP0 = generateMessages(numEvents);
    List<String> outgoingMessagesP1 = generateMessages(numEvents);

    // Set configs
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), streamName);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, systemName, streamName), EVENTHUB_NAMESPACE);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, systemName, streamName), EVENTHUB_KEY_NAME);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, systemName, streamName), EVENTHUB_KEY);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName), EVENTHUB_ENTITY1);
    // mod 2 on the partitionid to simulate consistent hashing
    configMap.put(String.format(EventHubConfig.CONFIG_PRODUCER_PARTITION_METHOD, systemName),
            PartitioningMethod.EVENT_HUB_HASHING.toString());

    MockEventHubClientManagerFactory factory = new MockEventHubClientManagerFactory();

    EventHubSystemProducer producer =
            new EventHubSystemProducer(new EventHubConfig(configMap), systemName, factory, interceptor, testMetrics);

    SystemStream systemStream = new SystemStream(systemName, streamName);
    producer.register(streamName);
    producer.start();

    outgoingMessagesP0.forEach(message ->
            producer.send(streamName, new OutgoingMessageEnvelope(systemStream, partitionId0, null, message.getBytes())));
    outgoingMessagesP1.forEach(message ->
            producer.send(streamName, new OutgoingMessageEnvelope(systemStream, partitionId1, null, message.getBytes())));

    // Retrieve sent data
    List<String> receivedData0 = factory.getSentData(systemName, streamName, 0)
            .stream().map(eventData -> new String(eventData.getBytes())).collect(Collectors.toList());
    List<String> receivedData1 = factory.getSentData(systemName, streamName, 1)
            .stream().map(eventData -> new String(eventData.getBytes())).collect(Collectors.toList());

    Assert.assertTrue(outgoingMessagesP0.equals(receivedData0));
    Assert.assertTrue(outgoingMessagesP1.equals(receivedData1));
  }
}
