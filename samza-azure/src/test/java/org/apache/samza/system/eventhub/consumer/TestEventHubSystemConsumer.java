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

package org.apache.samza.system.eventhub.consumer;


import com.microsoft.azure.eventhubs.*;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.Counter;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.*;
import org.apache.samza.system.eventhub.admin.PassThroughInterceptor;
import org.apache.samza.system.eventhub.producer.SwapFirstLastByteInterceptor;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.samza.system.eventhub.MockEventHubConfigFactory.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({EventHubRuntimeInformation.class, EventHubPartitionRuntimeInformation.class,
        EventHubClient.class, PartitionReceiver.class, PartitionSender.class})
public class TestEventHubSystemConsumer {
  private static final String MOCK_ENTITY_1 = "mocktopic1";
  private static final String MOCK_ENTITY_2 = "mocktopic2";

  private void verifyEvents(List<IncomingMessageEnvelope> messages, List<EventData> eventDataList) {
    verifyEvents(messages, eventDataList, new PassThroughInterceptor());
  }

  private void verifyEvents(List<IncomingMessageEnvelope> messages, List<EventData> eventDataList, Interceptor interceptor) {
    Assert.assertEquals(messages.size(), eventDataList.size());
    for (int i = 0; i < messages.size(); i++) {
      IncomingMessageEnvelope message = messages.get(i);
      EventData eventData = eventDataList.get(i);
      Assert.assertEquals(message.getKey(), eventData.getSystemProperties().getPartitionKey());
      Assert.assertEquals(message.getMessage(), interceptor.intercept(eventData.getBytes()));
      Assert.assertEquals(message.getOffset(), eventData.getSystemProperties().getOffset());
    }
  }

  @Test
  public void testMultipleRegistersToSameSSP() throws Exception {
    String systemName = "eventhubs";
    String streamName = "testStream";
    int numEvents = 10; // needs to be less than BLOCKING_QUEUE_SIZE
    int partitionId = 0;

    TestMetricsRegistry testMetrics = new TestMetricsRegistry();
    Map<SystemStreamPartition, List<EventData>> eventData = new HashMap<>();
    SystemStreamPartition ssp = new SystemStreamPartition(systemName, streamName, new Partition(partitionId));
    Map<String, Interceptor> interceptors = new HashMap<>();
    interceptors.put(streamName, new PassThroughInterceptor());

    // create EventData
    List<EventData> singlePartitionEventData = MockEventData.generateEventData(numEvents);
    eventData.put(ssp, singlePartitionEventData);

    // Set configs
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), streamName);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, systemName, streamName), EVENTHUB_NAMESPACE);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, systemName, streamName), EVENTHUB_KEY_NAME);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, systemName, streamName), EVENTHUB_KEY);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName), MOCK_ENTITY_1);
    MapConfig config = new MapConfig(configMap);

    MockEventHubClientManagerFactory eventHubClientWrapperFactory = new MockEventHubClientManagerFactory(eventData);

    EventHubSystemConsumer consumer =
            new EventHubSystemConsumer(new EventHubConfig(config), systemName, eventHubClientWrapperFactory, interceptors,
                    testMetrics);
    consumer.register(ssp, "1");
    consumer.register(ssp, EventHubSystemConsumer.END_OF_STREAM);
    consumer.register(ssp, EventHubSystemConsumer.START_OF_STREAM);
    consumer.start();

    Assert.assertEquals(EventHubSystemConsumer.START_OF_STREAM,
            eventHubClientWrapperFactory.getPartitionOffset(String.valueOf(partitionId)));
  }

  @Test
  public void testSinglePartitionConsumptionHappyPath() throws Exception {
    String systemName = "eventhubs";
    String streamName = "testStream";
    int numEvents = 10; // needs to be less than BLOCKING_QUEUE_SIZE
    int partitionId = 0;

    TestMetricsRegistry testMetrics = new TestMetricsRegistry();
    Map<SystemStreamPartition, List<EventData>> eventData = new HashMap<>();
    SystemStreamPartition ssp = new SystemStreamPartition(systemName, streamName, new Partition(partitionId));
    Map<String, Interceptor> interceptors = new HashMap<>();
    interceptors.put(streamName, new PassThroughInterceptor());

    // create EventData
    List<EventData> singlePartitionEventData = MockEventData.generateEventData(numEvents);
    eventData.put(ssp, singlePartitionEventData);

    // Set configs
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), streamName);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, systemName, streamName), EVENTHUB_NAMESPACE);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, systemName, streamName), EVENTHUB_KEY_NAME);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, systemName, streamName), EVENTHUB_KEY);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName), MOCK_ENTITY_1);
    MapConfig config = new MapConfig(configMap);

    MockEventHubClientManagerFactory eventHubClientWrapperFactory = new MockEventHubClientManagerFactory(eventData);

    EventHubSystemConsumer consumer =
            new EventHubSystemConsumer(new EventHubConfig(config), systemName, eventHubClientWrapperFactory, interceptors,
                    testMetrics);
    consumer.register(ssp, EventHubSystemConsumer.END_OF_STREAM);
    consumer.start();

    // Mock received data from EventHub
    eventHubClientWrapperFactory.sendToHandlers(consumer.streamPartitionHandlers);

    List<IncomingMessageEnvelope> result = consumer.poll(Collections.singleton(ssp), 1000).get(ssp);

    verifyEvents(result, singlePartitionEventData);
    Assert.assertEquals(testMetrics.getCounters(streamName).size(), 3);
    Assert.assertEquals(testMetrics.getGauges(streamName).size(), 2);
    Map<String, Counter> counters =
            testMetrics.getCounters(streamName).stream().collect(Collectors.toMap(Counter::getName, Function.identity()));

    Assert.assertEquals(counters.get(EventHubSystemConsumer.EVENT_READ_RATE).getCount(), numEvents);
    Assert.assertEquals(counters.get(EventHubSystemConsumer.READ_ERRORS).getCount(), 0);
  }

  @Test
  public void testSinglePartitionConsumptionInterceptor() throws Exception {
    String systemName = "eventhubs";
    String streamName = "testStream";
    int numEvents = 10; // needs to be less than BLOCKING_QUEUE_SIZE
    int partitionId = 0;
    Interceptor interceptor = new SwapFirstLastByteInterceptor();

    TestMetricsRegistry testMetrics = new TestMetricsRegistry();
    Map<SystemStreamPartition, List<EventData>> eventData = new HashMap<>();
    SystemStreamPartition ssp = new SystemStreamPartition(systemName, streamName, new Partition(partitionId));
    Map<String, Interceptor> interceptors = new HashMap<>();
    interceptors.put(streamName, interceptor);

    // create EventData
    List<EventData> singlePartitionEventData = MockEventData.generateEventData(numEvents);
    eventData.put(ssp, singlePartitionEventData);

    // Set configs
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), streamName);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, systemName, streamName), EVENTHUB_NAMESPACE);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, systemName, streamName), EVENTHUB_KEY_NAME);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, systemName, streamName), EVENTHUB_KEY);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName), MOCK_ENTITY_1);
    MapConfig config = new MapConfig(configMap);

    MockEventHubClientManagerFactory eventHubClientWrapperFactory = new MockEventHubClientManagerFactory(eventData);

    EventHubSystemConsumer consumer =
            new EventHubSystemConsumer(new EventHubConfig(config), systemName, eventHubClientWrapperFactory, interceptors,
                    testMetrics);
    consumer.register(ssp, EventHubSystemConsumer.END_OF_STREAM);
    consumer.start();

    // Mock received data from EventHub
    eventHubClientWrapperFactory.sendToHandlers(consumer.streamPartitionHandlers);

    List<IncomingMessageEnvelope> result = consumer.poll(Collections.singleton(ssp), 1000).get(ssp);

    verifyEvents(result, singlePartitionEventData, interceptor);
    Assert.assertEquals(testMetrics.getCounters(streamName).size(), 3);
    Assert.assertEquals(testMetrics.getGauges(streamName).size(), 2);
    Map<String, Counter> counters =
            testMetrics.getCounters(streamName).stream().collect(Collectors.toMap(Counter::getName, Function.identity()));

    Assert.assertEquals(counters.get(EventHubSystemConsumer.EVENT_READ_RATE).getCount(), numEvents);
    Assert.assertEquals(counters.get(EventHubSystemConsumer.READ_ERRORS).getCount(), 0);
  }

  @Test
  public void testMultiPartitionConsumptionHappyPath() throws Exception {
    String systemName = "eventhubs";
    String streamName = "testStream";
    int numEvents = 10; // needs to be less than BLOCKING_QUEUE_SIZE
    int partitionId1 = 0;
    int partitionId2 = 1;
    TestMetricsRegistry testMetrics = new TestMetricsRegistry();
    Map<SystemStreamPartition, List<EventData>> eventData = new HashMap<>();
    SystemStreamPartition ssp1 = new SystemStreamPartition(systemName, streamName, new Partition(partitionId1));
    SystemStreamPartition ssp2 = new SystemStreamPartition(systemName, streamName, new Partition(partitionId2));
    Map<String, Interceptor> interceptor = new HashMap<>();
    interceptor.put(streamName, new PassThroughInterceptor());

    // create EventData
    List<EventData> singlePartitionEventData1 = MockEventData.generateEventData(numEvents);
    List<EventData> singlePartitionEventData2 = MockEventData.generateEventData(numEvents);
    eventData.put(ssp1, singlePartitionEventData1);
    eventData.put(ssp2, singlePartitionEventData2);

    // Set configs
    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), streamName);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName), MOCK_ENTITY_1);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, systemName, streamName), EVENTHUB_NAMESPACE);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, systemName, streamName), EVENTHUB_KEY_NAME);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, systemName, streamName), EVENTHUB_KEY);
    MapConfig config = new MapConfig(configMap);

    MockEventHubClientManagerFactory eventHubClientWrapperFactory = new MockEventHubClientManagerFactory(eventData);

    EventHubSystemConsumer consumer =
            new EventHubSystemConsumer(new EventHubConfig(config), systemName, eventHubClientWrapperFactory, interceptor,
                    testMetrics);
    consumer.register(ssp1, EventHubSystemConsumer.START_OF_STREAM);
    consumer.register(ssp2, EventHubSystemConsumer.START_OF_STREAM);
    consumer.start();

    // Mock received data from EventHub
    eventHubClientWrapperFactory.sendToHandlers(consumer.streamPartitionHandlers);

    Set<SystemStreamPartition> ssps = new HashSet<>();
    ssps.add(ssp1);
    ssps.add(ssp2);
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> results = consumer.poll(ssps, 1000);
    verifyEvents(results.get(ssp1), singlePartitionEventData1);
    verifyEvents(results.get(ssp2), singlePartitionEventData2);

    Assert.assertEquals(testMetrics.getCounters(streamName).size(), 3);
    Assert.assertEquals(testMetrics.getGauges(streamName).size(), 2);
    Map<String, Counter> counters =
            testMetrics.getCounters(streamName).stream().collect(Collectors.toMap(Counter::getName, Function.identity()));

    Assert.assertEquals(counters.get(EventHubSystemConsumer.EVENT_READ_RATE).getCount(), numEvents * 2);
    Assert.assertEquals(counters.get(EventHubSystemConsumer.READ_ERRORS).getCount(), 0);
  }

  @Test
  public void testMultiStreamsConsumptionHappyPath() throws Exception {
    String systemName = "eventhubs";
    String streamName1 = "testStream1";
    String streamName2 = "testStream2";
    int numEvents = 10; // needs to be less than BLOCKING_QUEUE_SIZE
    int partitionId = 0;
    TestMetricsRegistry testMetrics = new TestMetricsRegistry();
    Map<SystemStreamPartition, List<EventData>> eventData = new HashMap<>();
    SystemStreamPartition ssp1 = new SystemStreamPartition(systemName, streamName1, new Partition(partitionId));
    SystemStreamPartition ssp2 = new SystemStreamPartition(systemName, streamName2, new Partition(partitionId));
    Map<String, Interceptor> interceptor = new HashMap<>();
    interceptor.put(streamName1, new PassThroughInterceptor());
    interceptor.put(streamName2, new PassThroughInterceptor());

    List<EventData> singlePartitionEventData1 = MockEventData.generateEventData(numEvents);
    List<EventData> singlePartitionEventData2 = MockEventData.generateEventData(numEvents);
    eventData.put(ssp1, singlePartitionEventData1);
    eventData.put(ssp2, singlePartitionEventData2);

    Map<String, String> configMap = new HashMap<>();
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName),
            String.format("%s,%s", streamName1, streamName2));
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName1), MOCK_ENTITY_1);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, systemName, streamName1), EVENTHUB_NAMESPACE);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, systemName, streamName1), EVENTHUB_KEY_NAME);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, systemName, streamName1), EVENTHUB_KEY);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName2), MOCK_ENTITY_2);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_NAMESPACE, systemName, streamName2), EVENTHUB_NAMESPACE);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_KEY_NAME, systemName, streamName2), EVENTHUB_KEY_NAME);
    configMap.put(String.format(EventHubConfig.CONFIG_STREAM_SAS_TOKEN, systemName, streamName2), EVENTHUB_KEY);
    MapConfig config = new MapConfig(configMap);

    MockEventHubClientManagerFactory eventHubClientWrapperFactory = new MockEventHubClientManagerFactory(eventData);

    EventHubSystemConsumer consumer =
            new EventHubSystemConsumer(new EventHubConfig(config), systemName, eventHubClientWrapperFactory, interceptor,
                    testMetrics);

    consumer.register(ssp1, EventHubSystemConsumer.START_OF_STREAM);
    consumer.register(ssp2, EventHubSystemConsumer.START_OF_STREAM);
    consumer.start();

    // Mock received data from EventHub
    eventHubClientWrapperFactory.sendToHandlers(consumer.streamPartitionHandlers);

    Set<SystemStreamPartition> ssps = new HashSet<>();
    ssps.add(ssp1);
    ssps.add(ssp2);
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> results = consumer.poll(ssps, 1000);
    verifyEvents(results.get(ssp1), singlePartitionEventData1);
    verifyEvents(results.get(ssp2), singlePartitionEventData2);

    Assert.assertEquals(testMetrics.getCounters(streamName1).size(), 3);
    Assert.assertEquals(testMetrics.getGauges(streamName1).size(), 2);

    Assert.assertEquals(testMetrics.getCounters(streamName2).size(), 3);
    Assert.assertEquals(testMetrics.getGauges(streamName2).size(), 2);

    Map<String, Counter> counters1 =
            testMetrics.getCounters(streamName1).stream().collect(Collectors.toMap(Counter::getName, Function.identity()));

    Assert.assertEquals(counters1.get(EventHubSystemConsumer.EVENT_READ_RATE).getCount(), numEvents);
    Assert.assertEquals(counters1.get(EventHubSystemConsumer.READ_ERRORS).getCount(), 0);

    Map<String, Counter> counters2 =
            testMetrics.getCounters(streamName2).stream().collect(Collectors.toMap(Counter::getName, Function.identity()));

    Assert.assertEquals(counters2.get(EventHubSystemConsumer.EVENT_READ_RATE).getCount(), numEvents);
    Assert.assertEquals(counters2.get(EventHubSystemConsumer.READ_ERRORS).getCount(), 0);
  }
}
