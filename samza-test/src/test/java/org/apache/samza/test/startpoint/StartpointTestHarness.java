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

package org.apache.samza.test.startpoint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.startpoint.Startpoint;
import org.apache.samza.startpoint.StartpointManager;
import org.apache.samza.startpoint.StartpointOldest;
import org.apache.samza.startpoint.StartpointSpecific;
import org.apache.samza.startpoint.StartpointTimestamp;
import org.apache.samza.startpoint.StartpointUpcoming;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.test.StandaloneTestUtils;
import org.apache.samza.test.harness.IntegrationTestHarness;
import org.apache.samza.test.processor.SharedContextFactories;
import org.apache.samza.test.processor.TestTaskApplication;
import org.apache.samza.test.util.TestKafkaEvent;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;


public class StartpointTestHarness extends IntegrationTestHarness {

  private static final Logger LOGGER = LoggerFactory.getLogger(StartpointTestHarness.class);

  private static final int NUM_KAFKA_EVENTS = 300;
  private static final int ZK_TEST_PARTITION_COUNT = 5;
  private static final String TEST_SYSTEM = "TestSystemName";
  private static final String TEST_SSP_GROUPER_FACTORY = "org.apache.samza.container.grouper.stream.GroupByPartitionFactory";
  private static final String TEST_TASK_GROUPER_FACTORY = "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory";
  private static final String TEST_JOB_COORDINATOR_FACTORY = "org.apache.samza.zk.ZkJobCoordinatorFactory";
  private static final String TEST_SYSTEM_FACTORY = "org.apache.samza.system.kafka.KafkaSystemFactory";
  private static final String TASK_SHUTDOWN_MS = "10000";
  private static final String JOB_DEBOUNCE_TIME_MS = "10000";
  private static final String BARRIER_TIMEOUT_MS = "10000";
  private static final String[] PROCESSOR_IDS = new String[] {"0000000000", "0000000001", "0000000002", "0000000003"};

  private String inputKafkaTopic1;
  private String inputKafkaTopic2;
  private String inputKafkaTopic3;
  private String inputKafkaTopic4;
  private String outputKafkaTopic;
  private ApplicationConfig applicationConfig1;
  private ApplicationConfig applicationConfig2;
  private ApplicationConfig applicationConfig3;
  private ApplicationConfig applicationConfig4;
  private String testStreamAppName;
  private String testStreamAppId;

  @Rule
  public Timeout testTimeOutInMillis = new Timeout(150000, TimeUnit.MILLISECONDS);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Override
  public void setUp() {
    super.setUp();
    String uniqueTestId = UUID.randomUUID().toString();
    testStreamAppName = String.format("test-app-name-%s", uniqueTestId);
    testStreamAppId = String.format("test-app-id-%s", uniqueTestId);
    inputKafkaTopic1 = String.format("test-input-topic1-%s", uniqueTestId);
    inputKafkaTopic2 = String.format("test-input-topic2-%s", uniqueTestId);
    inputKafkaTopic3 = String.format("test-input-topic3-%s", uniqueTestId);
    inputKafkaTopic4 = String.format("test-input-topic4-%s", uniqueTestId);
    outputKafkaTopic = String.format("test-output-topic-%s", uniqueTestId);

    // Set up stream application config map with the given testStreamAppName, testStreamAppId and test kafka system
    // TODO: processorId should typically come up from a processorID generator as processor.id will be deprecated in 0.14.0+
    Map<String, String> configMap =
        buildStreamApplicationConfigMap(testStreamAppName, testStreamAppId);
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[0]);
    applicationConfig1 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[1]);
    applicationConfig2 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[2]);
    applicationConfig3 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[3]);
    applicationConfig4 = new ApplicationConfig(new MapConfig(configMap));

    ImmutableMap<String, Integer> topicToPartitionCount = ImmutableMap.<String, Integer>builder()
        .put(inputKafkaTopic1, ZK_TEST_PARTITION_COUNT)
        .put(inputKafkaTopic2, ZK_TEST_PARTITION_COUNT)
        .put(inputKafkaTopic3, ZK_TEST_PARTITION_COUNT)
        .put(inputKafkaTopic4, ZK_TEST_PARTITION_COUNT)
        .put(outputKafkaTopic, ZK_TEST_PARTITION_COUNT)
        .build();

    List<NewTopic> newTopics =
        topicToPartitionCount.keySet()
            .stream()
            .map(topic -> new NewTopic(topic, topicToPartitionCount.get(topic), (short) 1))
            .collect(Collectors.toList());

    assertTrue("Encountered errors during test setup. Failed to create topics.", createTopics(newTopics));
  }

  @Override
  public void tearDown() {
    deleteTopics(
        ImmutableList.of(inputKafkaTopic1, inputKafkaTopic2, inputKafkaTopic3, inputKafkaTopic4, outputKafkaTopic));
    SharedContextFactories.clearAll();
    super.tearDown();
  }

  @Test
  public void testStartpointSpecific() throws InterruptedException {
    Map<Integer, RecordMetadata> sentEvents1 =
        publishKafkaEventsWithDelayPerEvent(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0], Duration.ofMillis(2));

    ConcurrentHashMap<String, IncomingMessageEnvelope> recvEventsInputStartpointSpecific = new ConcurrentHashMap<>();

    CoordinatorStreamStore coordinatorStreamStore = createCoordinatorStreamStore(applicationConfig1);
    coordinatorStreamStore.init();
    StartpointManager startpointManager = new StartpointManager(coordinatorStreamStore);
    startpointManager.start();

    StartpointSpecific startpointSpecific = new StartpointSpecific(String.valueOf(sentEvents1.get(100).offset()));
    writeStartpoints(startpointManager, inputKafkaTopic1, ZK_TEST_PARTITION_COUNT, startpointSpecific);

    startpointManager.stop();
    coordinatorStreamStore.close();

    TestTaskApplication.TaskApplicationCallback processedCallback = (IncomingMessageEnvelope ime, TaskCallback callback) -> {
      try {
        String streamName = ime.getSystemStreamPartition().getStream();
        TestKafkaEvent testKafkaEvent = TestKafkaEvent.fromString((String) ime.getMessage());
        String eventIndex = testKafkaEvent.getEventData();
        if (inputKafkaTopic1.equals(streamName)) {
          recvEventsInputStartpointSpecific.put(eventIndex, ime);
        } else {
          throw new RuntimeException("Unexpected input stream: " + streamName);
        }
        callback.complete();
      } catch (Exception ex) {
        callback.failure(ex);
      }
    };

    CountDownLatch processedMessagesLatchStartpointSpecific = new CountDownLatch(100); // Just fetch a few messages
    CountDownLatch shutdownLatchStartpointSpecific = new CountDownLatch(1);

    TestTaskApplication testTaskApplicationStartpointSpecific =
        new TestTaskApplication(TEST_SYSTEM, inputKafkaTopic1, outputKafkaTopic,
            processedMessagesLatchStartpointSpecific, shutdownLatchStartpointSpecific, Optional.of(processedCallback));

    ApplicationRunner appRunner =
        ApplicationRunners.getApplicationRunner(testTaskApplicationStartpointSpecific, applicationConfig1);
    executeRun(appRunner, applicationConfig1);

    assertTrue(processedMessagesLatchStartpointSpecific.await(1, TimeUnit.MINUTES));

    appRunner.kill();
    appRunner.waitForFinish();

    assertTrue(shutdownLatchStartpointSpecific.await(1, TimeUnit.MINUTES));

    Integer startpointSpecificOffset = Integer.valueOf(startpointSpecific.getSpecificOffset());
    for (IncomingMessageEnvelope ime : recvEventsInputStartpointSpecific.values()) {
      Integer eventOffset = Integer.valueOf(ime.getOffset());
      String assertMsg = String.format("Expecting message offset: %d >= Startpoint specific offset: %d", eventOffset,
          startpointSpecificOffset);
      assertTrue(assertMsg, eventOffset >= startpointSpecificOffset);
    }
  }

  @Test
  public void testStartpointTimestamp() throws InterruptedException {
    Map<Integer, RecordMetadata> sentEvents2 =
        publishKafkaEventsWithDelayPerEvent(inputKafkaTopic2, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[1], Duration.ofMillis(2));

    ConcurrentHashMap<String, IncomingMessageEnvelope> recvEventsInputStartpointTimestamp = new ConcurrentHashMap<>();

    CoordinatorStreamStore coordinatorStreamStore = createCoordinatorStreamStore(applicationConfig1);
    coordinatorStreamStore.init();
    StartpointManager startpointManager = new StartpointManager(coordinatorStreamStore);
    startpointManager.start();

    StartpointTimestamp startpointTimestamp = new StartpointTimestamp(sentEvents2.get(150).timestamp());
    writeStartpoints(startpointManager, inputKafkaTopic2, ZK_TEST_PARTITION_COUNT, startpointTimestamp);

    startpointManager.stop();
    coordinatorStreamStore.close();

    TestTaskApplication.TaskApplicationCallback processedCallback = (IncomingMessageEnvelope ime, TaskCallback callback) -> {
      try {
        String streamName = ime.getSystemStreamPartition().getStream();
        TestKafkaEvent testKafkaEvent =
            TestKafkaEvent.fromString((String) ime.getMessage());
        String eventIndex = testKafkaEvent.getEventData();
        if (inputKafkaTopic2.equals(streamName)) {
          recvEventsInputStartpointTimestamp.put(eventIndex, ime);
        } else {
          throw new RuntimeException("Unexpected input stream: " + streamName);
        }
        callback.complete();
      } catch (Exception ex) {
        callback.failure(ex);
      }
    };

    CountDownLatch processedMessagesLatchStartpointTimestamp = new CountDownLatch(100); // Just fetch a few messages
    CountDownLatch shutdownLatchStartpointTimestamp = new CountDownLatch(1);

    TestTaskApplication testTaskApplicationStartpointTimestamp =
        new TestTaskApplication(TEST_SYSTEM, inputKafkaTopic2, outputKafkaTopic, processedMessagesLatchStartpointTimestamp,
            shutdownLatchStartpointTimestamp, Optional.of(processedCallback));

    ApplicationRunner appRunner =
        ApplicationRunners.getApplicationRunner(testTaskApplicationStartpointTimestamp, applicationConfig2);

    executeRun(appRunner, applicationConfig2);
    assertTrue(processedMessagesLatchStartpointTimestamp.await(1, TimeUnit.MINUTES));

    appRunner.kill();
    appRunner.waitForFinish();

    assertTrue(shutdownLatchStartpointTimestamp.await(1, TimeUnit.MINUTES));

    for (IncomingMessageEnvelope ime : recvEventsInputStartpointTimestamp.values()) {
      Integer eventOffset = Integer.valueOf(ime.getOffset());
      assertNotEquals(0, ime.getEventTime()); // sanity check
      String assertMsg = String.format("Expecting message timestamp: %d >= Startpoint timestamp: %d",
          ime.getEventTime(), startpointTimestamp.getTimestampOffset());
      assertTrue(assertMsg, ime.getEventTime() >= startpointTimestamp.getTimestampOffset());
    }
  }

  @Test
  public void testStartpointOldest() throws InterruptedException {
    publishKafkaEventsWithDelayPerEvent(inputKafkaTopic3, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[2], Duration.ofMillis(2));

    ConcurrentHashMap<String, IncomingMessageEnvelope> recvEventsInputStartpointOldest = new ConcurrentHashMap<>();

    CoordinatorStreamStore coordinatorStreamStore = createCoordinatorStreamStore(applicationConfig1);
    coordinatorStreamStore.init();
    StartpointManager startpointManager = new StartpointManager(coordinatorStreamStore);
    startpointManager.start();

    StartpointOldest startpointOldest = new StartpointOldest();
    writeStartpoints(startpointManager, inputKafkaTopic3, ZK_TEST_PARTITION_COUNT, startpointOldest);

    startpointManager.stop();
    coordinatorStreamStore.close();

    TestTaskApplication.TaskApplicationCallback processedCallback = (IncomingMessageEnvelope ime, TaskCallback callback) -> {
      try {
        String streamName = ime.getSystemStreamPartition().getStream();
        TestKafkaEvent testKafkaEvent =
            TestKafkaEvent.fromString((String) ime.getMessage());
        String eventIndex = testKafkaEvent.getEventData();
        if (inputKafkaTopic3.equals(streamName)) {
          recvEventsInputStartpointOldest.put(eventIndex, ime);
        } else {
          throw new RuntimeException("Unexpected input stream: " + streamName);
        }
        callback.complete();
      } catch (Exception ex) {
        callback.failure(ex);
      }
    };

    CountDownLatch processedMessagesLatchStartpointOldest = new CountDownLatch(NUM_KAFKA_EVENTS); // Fetch all since consuming from oldest
    CountDownLatch shutdownLatchStartpointOldest = new CountDownLatch(1);

    TestTaskApplication testTaskApplicationStartpointOldest =
        new TestTaskApplication(TEST_SYSTEM, inputKafkaTopic3, outputKafkaTopic, processedMessagesLatchStartpointOldest,
            shutdownLatchStartpointOldest, Optional.of(processedCallback));

    ApplicationRunner appRunner =
        ApplicationRunners.getApplicationRunner(testTaskApplicationStartpointOldest, applicationConfig3);
    executeRun(appRunner, applicationConfig3);

    assertTrue(processedMessagesLatchStartpointOldest.await(1, TimeUnit.MINUTES));

    appRunner.kill();
    appRunner.waitForFinish();

    assertTrue(shutdownLatchStartpointOldest.await(1, TimeUnit.MINUTES));

    assertEquals("Expecting to have processed all the events", NUM_KAFKA_EVENTS, recvEventsInputStartpointOldest.size());
  }

  @Test
  public void testStartpointUpcoming() throws InterruptedException {
    publishKafkaEventsWithDelayPerEvent(inputKafkaTopic4, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[3], Duration.ofMillis(2));

    ConcurrentHashMap<String, IncomingMessageEnvelope> recvEventsInputStartpointUpcoming = new ConcurrentHashMap<>();

    CoordinatorStreamStore coordinatorStreamStore = createCoordinatorStreamStore(applicationConfig1);
    coordinatorStreamStore.init();
    StartpointManager startpointManager = new StartpointManager(coordinatorStreamStore);
    startpointManager.start();

    StartpointUpcoming startpointUpcoming = new StartpointUpcoming();
    writeStartpoints(startpointManager, inputKafkaTopic4, ZK_TEST_PARTITION_COUNT, startpointUpcoming);

    startpointManager.stop();
    coordinatorStreamStore.close();

    TestTaskApplication.TaskApplicationCallback processedCallback = (IncomingMessageEnvelope ime, TaskCallback callback) -> {
      try {
        String streamName = ime.getSystemStreamPartition().getStream();
        TestKafkaEvent testKafkaEvent =
            TestKafkaEvent.fromString((String) ime.getMessage());
        String eventIndex = testKafkaEvent.getEventData();
        if (inputKafkaTopic4.equals(streamName)) {
          recvEventsInputStartpointUpcoming.put(eventIndex, ime);
        } else {
          throw new RuntimeException("Unexpected input stream: " + streamName);
        }
        callback.complete();
      } catch (Exception ex) {
        callback.failure(ex);
      }
    };


    CountDownLatch processedMessagesLatchStartpointUpcoming = new CountDownLatch(5); // Expecting none, so just attempt a small number of fetches.
    CountDownLatch shutdownLatchStartpointUpcoming = new CountDownLatch(1);

    TestTaskApplication testTaskApplicationStartpointUpcoming =
        new TestTaskApplication(TEST_SYSTEM, inputKafkaTopic4, outputKafkaTopic, processedMessagesLatchStartpointUpcoming,
            shutdownLatchStartpointUpcoming, Optional.of(processedCallback));

    // Startpoint upcoming
    ApplicationRunner appRunner =
        ApplicationRunners.getApplicationRunner(testTaskApplicationStartpointUpcoming, applicationConfig4);
    executeRun(appRunner, applicationConfig4);

    assertFalse("Expecting to timeout and not process any old messages.", processedMessagesLatchStartpointUpcoming.await(15, TimeUnit.SECONDS));
    assertEquals("Expecting not to process any old messages.", 0, recvEventsInputStartpointUpcoming.size());

    appRunner.kill();
    appRunner.waitForFinish();

    assertTrue(shutdownLatchStartpointUpcoming.await(1, TimeUnit.MINUTES));
  }

  // Sends each event synchronously and returns map of event index to event metadata of the sent record/event. Adds the specified delay between sends.
  private Map<Integer, RecordMetadata> publishKafkaEventsWithDelayPerEvent(String topic, int startIndex, int endIndex, String streamProcessorId, Duration delay) {
    HashMap<Integer, RecordMetadata> eventsMetadata = new HashMap<>();
    for (int eventIndex = startIndex; eventIndex < endIndex; eventIndex++) {
      try {
        LOGGER.info("Publish kafka event with index : {} for stream processor: {}.", eventIndex, streamProcessorId);
        TestKafkaEvent testKafkaEvent =
            new TestKafkaEvent(streamProcessorId, String.valueOf(eventIndex));
        Thread.sleep(delay.toMillis());
        Future<RecordMetadata> send = producer.send(new ProducerRecord(topic,
            testKafkaEvent.toString().getBytes()));
        eventsMetadata.put(eventIndex, send.get(5, TimeUnit.SECONDS));
      } catch (Exception  e) {
        LOGGER.error("Publishing to kafka topic: {} resulted in exception: {}.", new Object[]{topic, e});
        throw new SamzaException(e);
      }
    }
    producer.flush();
    return eventsMetadata;
  }

  private void writeStartpoints(StartpointManager startpointManager, String streamName, Integer partitionCount, Startpoint startpoint) {
    for (int p = 0; p < partitionCount; p++) {
      startpointManager.writeStartpoint(new SystemStreamPartition(TEST_SYSTEM, streamName, new Partition(p)), startpoint);
    }
  }

  private static CoordinatorStreamStore createCoordinatorStreamStore(Config applicationConfig) {
    SystemStream coordinatorSystemStream = CoordinatorStreamUtil.getCoordinatorSystemStream(applicationConfig);
    SystemAdmins systemAdmins = new SystemAdmins(applicationConfig);
    SystemAdmin coordinatorSystemAdmin = systemAdmins.getSystemAdmin(coordinatorSystemStream.getSystem());
    coordinatorSystemAdmin.start();
    CoordinatorStreamUtil.createCoordinatorStream(coordinatorSystemStream, coordinatorSystemAdmin);
    coordinatorSystemAdmin.stop();
    return new CoordinatorStreamStore(applicationConfig, new NoOpMetricsRegistry());
  }

  private Map<String, String> buildStreamApplicationConfigMap(String appName, String appId) {
    String coordinatorSystemName = "coordinatorSystem";
    Map<String, String> config = new HashMap<>();
    config.put(ZkConfig.ZK_CONSENSUS_TIMEOUT_MS, BARRIER_TIMEOUT_MS);
    config.put(JobConfig.JOB_DEFAULT_SYSTEM, TEST_SYSTEM);
    config.put(TaskConfig.IGNORED_EXCEPTIONS, "*");
    config.put(ZkConfig.ZK_CONNECT, zkConnect());
    config.put(JobConfig.SSP_GROUPER_FACTORY, TEST_SSP_GROUPER_FACTORY);
    config.put(TaskConfig.GROUPER_FACTORY, TEST_TASK_GROUPER_FACTORY);
    config.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, TEST_JOB_COORDINATOR_FACTORY);
    config.put(ApplicationConfig.APP_NAME, appName);
    config.put(ApplicationConfig.APP_ID, appId);
    config.put("app.runner.class", "org.apache.samza.runtime.LocalApplicationRunner");
    config.put(String.format("systems.%s.samza.factory", TEST_SYSTEM), TEST_SYSTEM_FACTORY);
    config.put(JobConfig.JOB_NAME, appName);
    config.put(JobConfig.JOB_ID, appId);
    config.put(TaskConfig.TASK_SHUTDOWN_MS, TASK_SHUTDOWN_MS);
    config.put(TaskConfig.DROP_PRODUCER_ERRORS, "true");
    config.put(JobConfig.JOB_DEBOUNCE_TIME_MS, JOB_DEBOUNCE_TIME_MS);
    config.put(JobConfig.MONITOR_PARTITION_CHANGE_FREQUENCY_MS, "1000");
    config.put("job.coordinator.system", coordinatorSystemName);
    config.put("job.coordinator.replication.factor", "1");

    Map<String, String> samzaContainerConfig = ImmutableMap.<String, String>builder().putAll(config).build();
    Map<String, String> applicationConfig = Maps.newHashMap(samzaContainerConfig);
    applicationConfig.putAll(
        StandaloneTestUtils.getKafkaSystemConfigs(coordinatorSystemName, bootstrapServers(), zkConnect(), null, StandaloneTestUtils.SerdeAlias.STRING, true));
    applicationConfig.putAll(StandaloneTestUtils.getKafkaSystemConfigs(TEST_SYSTEM, bootstrapServers(), zkConnect(), null, StandaloneTestUtils.SerdeAlias.STRING, true));
    return applicationConfig;
  }
}
