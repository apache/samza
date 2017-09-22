package org.apache.samza.system.eventhub.consumer;


import com.microsoft.azure.eventhubs.EventData;
import org.apache.samza.Partition;
import org.apache.samza.metrics.Counter;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.eventhub.EventDataWrapper;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.apache.samza.system.eventhub.MockEventData;
import org.apache.samza.system.eventhub.TestMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TestEventHubSystemConsumer {
    private final String MOCK_ENTITY_1 = "mocktopic1";
    private final String MOCK_ENTITY_2 = "mocktopic2";

    private void verifyEvents(List<IncomingMessageEnvelope> messages, List<EventData> eventDataList) {
        Assert.assertEquals(messages.size(), eventDataList.size());
        for (int i = 0; i < messages.size(); i++) {
            IncomingMessageEnvelope message = messages.get(i);
            EventData eventData = eventDataList.get(i);
            Assert.assertEquals(message.getKey(), eventData.getSystemProperties().getPartitionKey());
            Assert.assertEquals(((EventDataWrapper) message.getMessage()).getDecryptedBody(), eventData.getBody());
            Assert.assertEquals(message.getOffset(), eventData.getSystemProperties().getOffset());
        }
    }

    @Test
    public void testSinglePartitionConsumptionHappyPath() throws Exception {
        String systemName = "eventhubs";
        String streamName = "testStream";
        int numEvents = 10; // needs to be less than BLOCKING_QUEUE_SIZE
        int partitionId = 0;

        TestMetricsRegistry testMetrics = new TestMetricsRegistry();
        Map<String, Map<Integer, List<EventData>>> eventData = new HashMap<>();
        Map<Integer, List<EventData>> singleTopicEventData = new HashMap<>();

        List<EventData> singlePartitionEventData = MockEventData.generateEventData(numEvents);
        singleTopicEventData.put(partitionId, singlePartitionEventData);
        eventData.put(MOCK_ENTITY_1, singleTopicEventData);
        EventHubEntityConnectionFactory connectionFactory = new MockEventHubEntityConnectionFactory(eventData);

        Map<String, String> configMap = new HashMap<>();
        configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), streamName);
        configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName), MOCK_ENTITY_1);
        configMap.put(String.format(EventHubConfig.CONFIG_STREAM_CONSUMER_START_POSITION, systemName, streamName), "latest");
        EventHubSystemConsumer consumer =
                new EventHubSystemConsumer(new EventHubConfig(configMap, systemName), connectionFactory, testMetrics);

        SystemStreamPartition ssp = new SystemStreamPartition(systemName, streamName, new Partition(partitionId));
        consumer.register(ssp, null);
        consumer.start();
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
    public void testMultiPartitionConsumptionHappyPath() throws Exception {
        String systemName = "eventhubs";
        String streamName = "testStream";
        int numEvents = 10; // needs to be less than BLOCKING_QUEUE_SIZE
        int partitionId1 = 0;
        int partitionId2 = 1;
        TestMetricsRegistry testMetrics = new TestMetricsRegistry();

        Map<String, Map<Integer, List<EventData>>> eventData = new HashMap<>();
        Map<Integer, List<EventData>> singleTopicEventData = new HashMap<>();
        List<EventData> singlePartitionEventData1 = MockEventData.generateEventData(numEvents);
        List<EventData> singlePartitionEventData2 = MockEventData.generateEventData(numEvents);
        singleTopicEventData.put(partitionId1, singlePartitionEventData1);
        singleTopicEventData.put(partitionId2, singlePartitionEventData2);
        eventData.put(MOCK_ENTITY_1, singleTopicEventData);
        EventHubEntityConnectionFactory connectionFactory = new MockEventHubEntityConnectionFactory(eventData);

        Map<String, String> configMap = new HashMap<>();
        configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName), streamName);
        configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName), MOCK_ENTITY_1);
        EventHubSystemConsumer consumer =
                new EventHubSystemConsumer(new EventHubConfig(configMap, systemName), connectionFactory, testMetrics);

        SystemStreamPartition ssp1 = new SystemStreamPartition(systemName, streamName, new Partition(partitionId1));
        consumer.register(ssp1, EventHubSystemConsumer.START_OF_STREAM);
        SystemStreamPartition ssp2 = new SystemStreamPartition(systemName, streamName, new Partition(partitionId2));
        consumer.register(ssp2, EventHubSystemConsumer.START_OF_STREAM);
        consumer.start();
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

        Map<String, Map<Integer, List<EventData>>> eventData = new HashMap<>();
        Map<Integer, List<EventData>> singleTopicEventData1 = new HashMap<>();
        List<EventData> singlePartitionEventData1 = MockEventData.generateEventData(numEvents);
        singleTopicEventData1.put(partitionId, singlePartitionEventData1);
        eventData.put(MOCK_ENTITY_1, singleTopicEventData1);
        Map<Integer, List<EventData>> singleTopicEventData2 = new HashMap<>();
        List<EventData> singlePartitionEventData2 = MockEventData.generateEventData(numEvents);
        singleTopicEventData2.put(partitionId, singlePartitionEventData2);
        eventData.put(MOCK_ENTITY_2, singleTopicEventData2);
        EventHubEntityConnectionFactory connectionFactory = new MockEventHubEntityConnectionFactory(eventData);

        Map<String, String> configMap = new HashMap<>();
        configMap.put(String.format(EventHubConfig.CONFIG_STREAM_LIST, systemName),
                String.format("%s,%s", streamName1, streamName2));
        configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName1), MOCK_ENTITY_1);
        configMap.put(String.format(EventHubConfig.CONFIG_STREAM_ENTITYPATH, systemName, streamName2), MOCK_ENTITY_2);
        EventHubSystemConsumer consumer =
                new EventHubSystemConsumer(new EventHubConfig(configMap, systemName), connectionFactory, testMetrics);

        SystemStreamPartition ssp1 = new SystemStreamPartition(systemName, streamName1, new Partition(partitionId));
        consumer.register(ssp1, EventHubSystemConsumer.START_OF_STREAM);
        SystemStreamPartition ssp2 = new SystemStreamPartition(systemName, streamName2, new Partition(partitionId));
        consumer.register(ssp2, EventHubSystemConsumer.START_OF_STREAM);
        consumer.start();
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
