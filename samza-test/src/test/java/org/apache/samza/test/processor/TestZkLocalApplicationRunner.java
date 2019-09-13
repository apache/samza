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

package org.apache.samza.test.processor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.ClusterManagerConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.stream.CoordinatorStreamValueSerde;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.JobModelUtil;
import org.apache.samza.job.model.TaskMode;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metadatastore.MetadataStoreFactory;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.runtime.LocalApplicationRunner;
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
import org.apache.samza.util.CoordinatorStreamUtil;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.samza.util.ReflectionUtil;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.apache.samza.zk.ZkKeyBuilder;
import org.apache.samza.zk.ZkMetadataStore;
import org.apache.samza.zk.ZkStringSerializer;
import org.apache.samza.zk.ZkUtils;
import org.junit.Assert;
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


/**
 * Integration tests for {@link org.apache.samza.runtime.LocalApplicationRunner} with {@link ZkJobCoordinatorFactory}.
 *
 * Brings up embedded ZooKeeper, Kafka broker and launches multiple {@link org.apache.samza.application.StreamApplication}
 * through {@link org.apache.samza.runtime.LocalApplicationRunner} to verify the guarantees made in stand alone execution
 * environment.
 */
public class TestZkLocalApplicationRunner extends IntegrationTestHarness {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestZkLocalApplicationRunner.class);

  private static final int NUM_KAFKA_EVENTS = 300;
  private static final int ZK_CONNECTION_TIMEOUT_MS = 5000;
  private static final int ZK_SESSION_TIMEOUT_MS = 10000;
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
  private String inputSinglePartitionKafkaTopic;
  private String outputSinglePartitionKafkaTopic;
  private ZkUtils zkUtils;
  private ApplicationConfig applicationConfig1;
  private ApplicationConfig applicationConfig2;
  private ApplicationConfig applicationConfig3;
  private ApplicationConfig applicationConfig4;
  private String testStreamAppName;
  private String testStreamAppId;
  private MetadataStore zkMetadataStore;

  @Rule
  public Timeout testTimeOutInMillis = new Timeout(150000);

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
    inputSinglePartitionKafkaTopic = String.format("test-input-single-partition-topic-%s", uniqueTestId);
    outputSinglePartitionKafkaTopic = String.format("test-output-single-partition-topic-%s", uniqueTestId);

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

    ZkClient zkClient = new ZkClient(zkConnect(), ZK_CONNECTION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS, new ZkStringSerializer());
    ZkKeyBuilder zkKeyBuilder = new ZkKeyBuilder(ZkJobCoordinatorFactory.getJobCoordinationZkPath(applicationConfig1));
    zkUtils = new ZkUtils(zkKeyBuilder, zkClient, ZK_CONNECTION_TIMEOUT_MS, ZK_SESSION_TIMEOUT_MS, new NoOpMetricsRegistry());
    zkUtils.connect();

    ImmutableMap<String, Integer> topicToPartitionCount = ImmutableMap.<String, Integer>builder()
        .put(inputSinglePartitionKafkaTopic, 1)
        .put(outputSinglePartitionKafkaTopic, 1)
        .put(inputKafkaTopic1, 5)
        .put(inputKafkaTopic2, 5)
        .put(inputKafkaTopic3, 5)
        .put(inputKafkaTopic4, 5)
        .put(outputKafkaTopic, 5)
        .build();

    List<NewTopic> newTopics =
        topicToPartitionCount.keySet()
            .stream()
            .map(topic -> new NewTopic(topic, topicToPartitionCount.get(topic), (short) 1))
            .collect(Collectors.toList());

    assertTrue("Encountered errors during test setup. Failed to create topics.", createTopics(newTopics));
    zkMetadataStore = new ZkMetadataStore(zkUtils.getKeyBuilder().getRootPath(), new MapConfig(configMap), new NoOpMetricsRegistry());
  }

  @Override
  public void tearDown() {
    deleteTopics(ImmutableList.of(inputKafkaTopic1, inputKafkaTopic2, inputKafkaTopic3, inputKafkaTopic4, outputKafkaTopic,
        inputSinglePartitionKafkaTopic, outputSinglePartitionKafkaTopic));
    SharedContextFactories.clearAll();
    zkUtils.close();
    super.tearDown();
  }

  private void publishKafkaEvents(String topic, int startIndex, int endIndex, String streamProcessorId) {
    for (int eventIndex = startIndex; eventIndex < endIndex; eventIndex++) {
      try {
        LOGGER.info("Publish kafka event with index : {} for stream processor: {}.", eventIndex, streamProcessorId);
        TestStreamApplication.TestKafkaEvent testKafkaEvent =
            new TestStreamApplication.TestKafkaEvent(streamProcessorId, String.valueOf(eventIndex));
        producer.send(new ProducerRecord(topic, testKafkaEvent.toString().getBytes()));
      } catch (Exception  e) {
        LOGGER.error("Publishing to kafka topic: {} resulted in exception: {}.", new Object[]{topic, e});
        throw new SamzaException(e);
      }
    }
    producer.flush();
  }

  // Sends each event synchronously and returns map of event index to event metadata of the sent record/event. Adds the specified delay between sends.
  private TreeMap<Integer, RecordMetadata> publishKafkaEventsWithDelayPerEvent(String topic, int startIndex, int endIndex, String streamProcessorId, Duration delay) {
    TreeMap<Integer, RecordMetadata> eventsMetadata = new TreeMap<>();
    for (int eventIndex = startIndex; eventIndex < endIndex; eventIndex++) {
      try {
        LOGGER.info("Publish kafka event with index : {} for stream processor: {}.", eventIndex, streamProcessorId);
        TestStreamApplication.TestKafkaEvent testKafkaEvent =
            new TestStreamApplication.TestKafkaEvent(streamProcessorId, String.valueOf(eventIndex));
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

  private Map<String, String> buildStreamApplicationConfigMap(String appName, String appId, boolean isBatch) {
    String coordinatorSystemName = "coordinatorSystem";
    Map<String, String> config = new HashMap<>();
    config.put(ZkConfig.ZK_CONSENSUS_TIMEOUT_MS, BARRIER_TIMEOUT_MS);
    config.put(JobConfig.JOB_DEFAULT_SYSTEM, TestZkLocalApplicationRunner.TEST_SYSTEM);
    config.put(TaskConfig.IGNORED_EXCEPTIONS, "*");
    config.put(ZkConfig.ZK_CONNECT, zkConnect());
    config.put(JobConfig.SSP_GROUPER_FACTORY, TEST_SSP_GROUPER_FACTORY);
    config.put(TaskConfig.GROUPER_FACTORY, TEST_TASK_GROUPER_FACTORY);
    config.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, TEST_JOB_COORDINATOR_FACTORY);
    config.put(ApplicationConfig.APP_NAME, appName);
    config.put(ApplicationConfig.APP_ID, appId);
    config.put("app.runner.class", "org.apache.samza.runtime.LocalApplicationRunner");
    config.put(String.format("systems.%s.samza.factory", TestZkLocalApplicationRunner.TEST_SYSTEM), TEST_SYSTEM_FACTORY);
    config.put(JobConfig.JOB_NAME, appName);
    config.put(JobConfig.JOB_ID, appId);
    config.put(TaskConfig.TASK_SHUTDOWN_MS, TASK_SHUTDOWN_MS);
    config.put(TaskConfig.DROP_PRODUCER_ERRORS, "true");
    config.put(JobConfig.JOB_DEBOUNCE_TIME_MS, JOB_DEBOUNCE_TIME_MS);
    config.put(JobConfig.MONITOR_PARTITION_CHANGE_FREQUENCY_MS, "1000");
    config.put(ClusterManagerConfig.HOST_AFFINITY_ENABLED, "true");
    config.put("job.coordinator.system", coordinatorSystemName);
    config.put("job.coordinator.replication.factor", "1");
    if (isBatch) {
      config.put(ApplicationConfig.APP_MODE, "BATCH");
      config.put(ClusterManagerConfig.HOST_AFFINITY_ENABLED, "false");
    }
    Map<String, String> samzaContainerConfig = ImmutableMap.<String, String>builder().putAll(config).build();
    Map<String, String> applicationConfig = Maps.newHashMap(samzaContainerConfig);
    applicationConfig.putAll(StandaloneTestUtils.getKafkaSystemConfigs(coordinatorSystemName, bootstrapServers(), zkConnect(), null, StandaloneTestUtils.SerdeAlias.STRING, true));
    applicationConfig.putAll(StandaloneTestUtils.getKafkaSystemConfigs(TestZkLocalApplicationRunner.TEST_SYSTEM, bootstrapServers(), zkConnect(), null, StandaloneTestUtils.SerdeAlias.STRING, true));
    return applicationConfig;
  }

  private Map<String, String> buildStreamApplicationConfigMap(String appName, String appId) {
    return buildStreamApplicationConfigMap(appName, appId, false);
  }

  /**
   * sspGrouper is set to GroupBySystemStreamPartitionFactory.
   * Run a stream application(appRunner1) consuming messages from input topic(effectively one container).
   *
   * In the callback triggered by appRunner1 after processing a message, bring up an another stream application(appRunner2).
   *
   * Assertions:
   *           A) JobModel generated before and after the addition of appRunner2 should be equal.
   *           B) Second stream application(appRunner2) should not join the group and process any message.
   */

  @Test
  public void shouldStopNewProcessorsJoiningGroupWhenNumContainersIsGreaterThanNumTasks() throws InterruptedException {
    // Set up kafka topics.
    publishKafkaEvents(inputSinglePartitionKafkaTopic, 0, NUM_KAFKA_EVENTS * 2, PROCESSOR_IDS[0]);

    // Configuration, verification variables
    MapConfig testConfig = new MapConfig(ImmutableMap.of(JobConfig.SSP_GROUPER_FACTORY,
        "org.apache.samza.container.grouper.stream.GroupBySystemStreamPartitionFactory", JobConfig.JOB_DEBOUNCE_TIME_MS, "10",
            ClusterManagerConfig.JOB_HOST_AFFINITY_ENABLED, "true"));
    // Declared as final array to update it from streamApplication callback(Variable should be declared final to access in lambda block).
    final JobModel[] previousJobModel = new JobModel[1];
    final String[] previousJobModelVersion = new String[1];
    AtomicBoolean hasSecondProcessorJoined = new AtomicBoolean(false);
    final CountDownLatch secondProcessorRegistered = new CountDownLatch(1);

    zkUtils.subscribeToProcessorChange((parentPath, currentChilds) -> {
        // When appRunner2 with id: PROCESSOR_IDS[1] is registered, run processing message in appRunner1.
        if (currentChilds.contains(PROCESSOR_IDS[1])) {
          secondProcessorRegistered.countDown();
        }
      });

    // Set up stream app appRunner2.
    CountDownLatch processedMessagesLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    Config localTestConfig2 = new MapConfig(applicationConfig2, testConfig);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputSinglePartitionKafkaTopic), outputSinglePartitionKafkaTopic, processedMessagesLatch,
        null, null, localTestConfig2), localTestConfig2);

    // Callback handler for appRunner1.
    TestStreamApplication.StreamApplicationCallback callback = m -> {
      if (hasSecondProcessorJoined.compareAndSet(false, true)) {
        previousJobModelVersion[0] = zkUtils.getJobModelVersion();
        previousJobModel[0] = JobModelUtil.readJobModel(previousJobModelVersion[0], zkMetadataStore);
        executeRun(appRunner2, localTestConfig2);
        try {
          // Wait for appRunner2 to register with zookeeper.
          secondProcessorRegistered.await();
        } catch (InterruptedException e) {
        }
      }
    };

    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS * 2);

    // Set up stream app appRunner1.
    Config localTestConfig1 = new MapConfig(applicationConfig1, testConfig);
    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputSinglePartitionKafkaTopic), outputSinglePartitionKafkaTopic, null,
        callback, kafkaEventsConsumedLatch, localTestConfig1), localTestConfig1);
    executeRun(appRunner1, localTestConfig1);

    kafkaEventsConsumedLatch.await();

    String currentJobModelVersion = zkUtils.getJobModelVersion();
    JobModel updatedJobModel = JobModelUtil.readJobModel(currentJobModelVersion, zkMetadataStore);

    // Job model before and after the addition of second stream processor should be the same.
    assertEquals(previousJobModel[0], updatedJobModel);
    assertEquals(new MapConfig(), updatedJobModel.getConfig());
    assertEquals(NUM_KAFKA_EVENTS, processedMessagesLatch.getCount());
    appRunner1.kill();
    appRunner1.waitForFinish();
    appRunner2.kill();
    appRunner2.waitForFinish();
    assertEquals(appRunner1.status(), ApplicationStatus.SuccessfulFinish);
    assertEquals(appRunner2.status(), ApplicationStatus.UnsuccessfulFinish);
  }

  /**
   * sspGrouper is set to AllSspToSingleTaskGrouperFactory (All ssps from input kafka topic are mapped to a single task per container).
   * AllSspToSingleTaskGrouperFactory should be used only with high-level consumers which do the partition management
   * by themselves. Using the factory with the consumers that do not do the partition management will result in
   * each processor/task consuming all the messages from all the partitions.
   * Run a stream application(streamApp1) consuming messages from input topic(effectively one container).
   *
   * In the callback triggered by streamApp1 after processing a message, bring up an another stream application(streamApp2).
   *
   * Assertions:
   *           A) JobModel generated before and after the addition of streamApp2 should not be equal.
   *           B) Second stream application(streamApp2) should join the group and process all the messages.
   */

  @Test
  public void shouldUpdateJobModelWhenNewProcessorJoiningGroupUsingAllSspToSingleTaskGrouperFactory() throws InterruptedException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS * 2, PROCESSOR_IDS[0]);

    // Configuration, verification variables
    MapConfig testConfig = new MapConfig(ImmutableMap.of(JobConfig.SSP_GROUPER_FACTORY,
        "org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory", JobConfig.JOB_DEBOUNCE_TIME_MS, "10", ClusterManagerConfig.HOST_AFFINITY_ENABLED, "false"));
    // Declared as final array to update it from streamApplication callback(Variable should be declared final to access in lambda block).
    final JobModel[] previousJobModel = new JobModel[1];
    final String[] previousJobModelVersion = new String[1];
    AtomicBoolean hasSecondProcessorJoined = new AtomicBoolean(false);
    final CountDownLatch secondProcessorRegistered = new CountDownLatch(1);

    zkUtils.subscribeToProcessorChange((parentPath, currentChilds) -> {
        // When appRunner2 with id: PROCESSOR_IDS[1] is registered, start processing message in appRunner1.
        if (currentChilds.contains(PROCESSOR_IDS[1])) {
          secondProcessorRegistered.countDown();
        }
      });

    // Set up appRunner2.
    CountDownLatch processedMessagesLatch = new CountDownLatch(NUM_KAFKA_EVENTS * 2);
    Config testAppConfig2 = new MapConfig(applicationConfig2, testConfig);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch, null,
        null, testAppConfig2), testAppConfig2);

    // Callback handler for appRunner1.
    TestStreamApplication.StreamApplicationCallback streamApplicationCallback = message -> {
      if (hasSecondProcessorJoined.compareAndSet(false, true)) {
        previousJobModelVersion[0] = zkUtils.getJobModelVersion();
        previousJobModel[0] = JobModelUtil.readJobModel(previousJobModelVersion[0], zkMetadataStore);
        executeRun(appRunner2, testAppConfig2);
        try {
          // Wait for appRunner2 to register with zookeeper.
          secondProcessorRegistered.await();
        } catch (InterruptedException e) {
        }
      }
    };

    // This is the latch for the messages received by appRunner1. Since appRunner1 is run first, it gets one event
    // redelivered due to re-balancing done by Zk after the appRunner2 joins (See the callback above).
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS * 2 + 1);

    // Set up stream app appRunner1.
    Config testAppConfig1 = new MapConfig(applicationConfig1, testConfig);
    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, null, streamApplicationCallback,
        kafkaEventsConsumedLatch, testAppConfig1), testAppConfig1);
    executeRun(appRunner1, testAppConfig1);

    kafkaEventsConsumedLatch.await();

    String currentJobModelVersion = zkUtils.getJobModelVersion();
    JobModel updatedJobModel = JobModelUtil.readJobModel(currentJobModelVersion, zkMetadataStore);

    // JobModelVersion check to verify that leader publishes new jobModel.
    assertTrue(Integer.parseInt(previousJobModelVersion[0]) < Integer.parseInt(currentJobModelVersion));

    // Job model before and after the addition of second stream processor should not be the same.
    assertTrue(!previousJobModel[0].equals(updatedJobModel));

    // Task names in the job model should be different but the set of partitions should be the same and each task name
    // should be assigned to a different container.
    assertEquals(new MapConfig(), previousJobModel[0].getConfig());
    assertEquals(previousJobModel[0].getContainers().get(PROCESSOR_IDS[0]).getTasks().size(), 1);
    assertEquals(new MapConfig(), updatedJobModel.getConfig());
    assertEquals(updatedJobModel.getContainers().get(PROCESSOR_IDS[0]).getTasks().size(), 1);
    assertEquals(updatedJobModel.getContainers().get(PROCESSOR_IDS[1]).getTasks().size(), 1);
    Map<TaskName, TaskModel> updatedTaskModelMap1 = updatedJobModel.getContainers().get(PROCESSOR_IDS[0]).getTasks();
    Map<TaskName, TaskModel> updatedTaskModelMap2 = updatedJobModel.getContainers().get(PROCESSOR_IDS[1]).getTasks();
    assertEquals(updatedTaskModelMap1.size(), 1);
    assertEquals(updatedTaskModelMap2.size(), 1);

    TaskModel taskModel1 = updatedTaskModelMap1.values().stream().findFirst().get();
    TaskModel taskModel2 = updatedTaskModelMap2.values().stream().findFirst().get();
    assertEquals(taskModel1.getSystemStreamPartitions(), taskModel2.getSystemStreamPartitions());
    assertFalse(taskModel1.getTaskName().getTaskName().equals(taskModel2.getTaskName().getTaskName()));

    processedMessagesLatch.await();

    assertEquals(ApplicationStatus.Running, appRunner2.status());
    appRunner1.kill();
    appRunner1.waitForFinish();
    appRunner2.kill();
    appRunner2.waitForFinish();
    assertEquals(ApplicationStatus.SuccessfulFinish, appRunner1.status());
  }

  @Test
  public void shouldReElectLeaderWhenLeaderDies() throws InterruptedException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic1, 0, 2 * NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create stream applications.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(2 * NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch3 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch,
        applicationConfig1), applicationConfig1);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch,
        applicationConfig2), applicationConfig2);
    ApplicationRunner appRunner3 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch3, null, kafkaEventsConsumedLatch,
        applicationConfig3), applicationConfig3);

    executeRun(appRunner1, applicationConfig1);
    executeRun(appRunner2, applicationConfig2);

    // Wait until all processors have processed a message.
    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // Verifications before killing the leader.
    String jobModelVersion = zkUtils.getJobModelVersion();
    JobModel jobModel = JobModelUtil.readJobModel(jobModelVersion, zkMetadataStore);
    assertEquals(2, jobModel.getContainers().size());
    assertEquals(Sets.newHashSet("0000000000", "0000000001"), jobModel.getContainers().keySet());
    assertEquals("1", jobModelVersion);

    List<String> processorIdsFromZK = zkUtils.getActiveProcessorsIDs(Arrays.asList(PROCESSOR_IDS));

    assertEquals(2, processorIdsFromZK.size());
    assertEquals(PROCESSOR_IDS[0], processorIdsFromZK.get(0));

    // Kill the leader. Since appRunner1 is the first to join the cluster, it's the leader.
    appRunner1.kill();
    appRunner1.waitForFinish();
    assertEquals(ApplicationStatus.SuccessfulFinish, appRunner1.status());

    kafkaEventsConsumedLatch.await();
    publishKafkaEvents(inputKafkaTopic1, 0, 2 * NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    executeRun(appRunner3, applicationConfig3);
    processedMessagesLatch3.await();

    // Verifications after killing the leader.
    processorIdsFromZK = zkUtils.getActiveProcessorsIDs(ImmutableList.of(PROCESSOR_IDS[1], PROCESSOR_IDS[2]));
    assertEquals(2, processorIdsFromZK.size());
    assertEquals(PROCESSOR_IDS[1], processorIdsFromZK.get(0));
    jobModelVersion = zkUtils.getJobModelVersion();
    jobModel = JobModelUtil.readJobModel(jobModelVersion, zkMetadataStore);
    assertEquals(Sets.newHashSet("0000000001", "0000000002"), jobModel.getContainers().keySet());
    assertEquals(2, jobModel.getContainers().size());

    appRunner2.kill();
    appRunner2.waitForFinish();
    assertEquals(ApplicationStatus.SuccessfulFinish, appRunner2.status());
    appRunner3.kill();
    appRunner3.waitForFinish();
    assertEquals(ApplicationStatus.SuccessfulFinish, appRunner3.status());
  }

  @Test
  public void shouldFailWhenNewProcessorJoinsWithSameIdAsExistingProcessor() throws InterruptedException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create StreamApplications.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch,
        applicationConfig1), applicationConfig1);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch,
        applicationConfig2), applicationConfig2);

    // Run stream applications.
    executeRun(appRunner1, applicationConfig1);
    executeRun(appRunner2, applicationConfig2);

    // Wait for message processing to run in both the processors.
    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // Create a stream app with same processor id as SP2 and run it. It should fail.
    publishKafkaEvents(inputKafkaTopic1, NUM_KAFKA_EVENTS, 2 * NUM_KAFKA_EVENTS, PROCESSOR_IDS[2]);
    kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    ApplicationRunner appRunner3 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, null, null, kafkaEventsConsumedLatch,
        applicationConfig2), applicationConfig2);
    // Fail when the duplicate processor joins.
    expectedException.expect(SamzaException.class);
    try {
      executeRun(appRunner3, applicationConfig2);
    } finally {
      appRunner1.kill();
      appRunner2.kill();

      appRunner1.waitForFinish();
      appRunner2.waitForFinish();
    }
  }

  public void addMessagesProcessed(List<TestStreamApplication.TestKafkaEvent> messagesProcessed, TestStreamApplication.TestKafkaEvent tke) {
    messagesProcessed.add(tke);
  }

  @Test
  public void testRollingUpgradeOfStreamApplicationsShouldGenerateSameJobModel() throws Exception {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    Map<String, String> configMap = buildStreamApplicationConfigMap(testStreamAppName, testStreamAppId);

    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[0]);
    Config applicationConfig1 = new MapConfig(configMap);

    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[1]);
    Config applicationConfig2 = new MapConfig(configMap);

    List<TestStreamApplication.TestKafkaEvent> messagesProcessed = new ArrayList<>();
    TestStreamApplication.StreamApplicationCallback streamApplicationCallback =
        (TestStreamApplication.TestKafkaEvent tke) -> addMessagesProcessed(messagesProcessed, tke);

    // Create StreamApplication from configuration.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, streamApplicationCallback, kafkaEventsConsumedLatch,
        applicationConfig1), applicationConfig1);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch,
        applicationConfig2), applicationConfig2);

    // Run stream application.
    executeRun(appRunner1, applicationConfig1);
    executeRun(appRunner2, applicationConfig2);

    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // Read job model before rolling upgrade.
    String jobModelVersion = zkUtils.getJobModelVersion();
    JobModel jobModel = JobModelUtil.readJobModel(jobModelVersion, zkMetadataStore);

    appRunner1.kill();
    appRunner1.waitForFinish();

    int lastProcessedMessageId = -1;
    for (TestStreamApplication.TestKafkaEvent message : messagesProcessed) {
      lastProcessedMessageId = Math.max(lastProcessedMessageId, Integer.parseInt(message.getEventData()));
    }
    messagesProcessed.clear();

    assertEquals(ApplicationStatus.SuccessfulFinish, appRunner1.status());

    processedMessagesLatch1 = new CountDownLatch(1);
    publishKafkaEvents(inputKafkaTopic1, NUM_KAFKA_EVENTS, 2 * NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);
    ApplicationRunner appRunner3 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch,
        applicationConfig1), applicationConfig1);
    executeRun(appRunner3, applicationConfig1);

    processedMessagesLatch1.await();

    // Read new job model after rolling upgrade.
    String newJobModelVersion = zkUtils.getJobModelVersion();
    JobModel newJobModel = JobModelUtil.readJobModel(newJobModelVersion, zkMetadataStore);

    assertEquals(Integer.parseInt(jobModelVersion) + 1, Integer.parseInt(newJobModelVersion));
    assertEquals(jobModel.getContainers(), newJobModel.getContainers());

    appRunner2.kill();
    appRunner2.waitForFinish();
    assertEquals(ApplicationStatus.SuccessfulFinish, appRunner2.status());
    appRunner3.kill();
    appRunner3.waitForFinish();
    assertEquals(ApplicationStatus.SuccessfulFinish, appRunner3.status());
  }

  @Test
  public void testShouldStopStreamApplicationWhenShutdownTimeOutIsLessThanContainerShutdownTime() throws Exception {
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    Map<String, String> configMap = buildStreamApplicationConfigMap(testStreamAppName, testStreamAppId);
    configMap.put(TaskConfig.TASK_SHUTDOWN_MS, "0");

    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[0]);
    Config applicationConfig1 = new MapConfig(configMap);

    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[1]);
    Config applicationConfig2 = new MapConfig(configMap);

    // Create StreamApplication from configuration.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch,
        applicationConfig1), applicationConfig1);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch,
        applicationConfig2), applicationConfig2);

    executeRun(appRunner1, applicationConfig1);
    executeRun(appRunner2, applicationConfig2);

    processedMessagesLatch1.await();
    processedMessagesLatch2.await();
    kafkaEventsConsumedLatch.await();

    // At this stage, both the processors are running and have drained the kakfa source.
    // Trigger re-balancing phase, by manually adding a new processor.

    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[2]);

    // Reset the task shutdown ms for 3rd application to give it ample time to shutdown cleanly
    configMap.put(TaskConfig.TASK_SHUTDOWN_MS, TASK_SHUTDOWN_MS);
    Config applicationConfig3 = new MapConfig(configMap);

    CountDownLatch processedMessagesLatch3 = new CountDownLatch(1);

    ApplicationRunner appRunner3 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch3, null, kafkaEventsConsumedLatch,
        applicationConfig3), applicationConfig3);
    executeRun(appRunner3, applicationConfig3);

    publishKafkaEvents(inputKafkaTopic1, NUM_KAFKA_EVENTS, 2 * NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    processedMessagesLatch3.await();
    appRunner1.waitForFinish();
    appRunner2.waitForFinish();

    /**
     * If the processing has started in the third stream processor, then other two stream processors should be stopped.
     */
    assertEquals(ApplicationStatus.UnsuccessfulFinish, appRunner1.status());
    assertEquals(ApplicationStatus.UnsuccessfulFinish, appRunner2.status());

    appRunner3.kill();
    appRunner3.waitForFinish();
    assertEquals(ApplicationStatus.SuccessfulFinish, appRunner3.status());
  }

  /**
   * A. Create a kafka topic with partition count set to 5.
   * B. Create and launch a samza application which consumes events from the kafka topic.
   * C. Validate that the {@link JobModel} contains 5 {@link SystemStreamPartition}'s.
   * D. Increase the partition count of the input kafka topic to 100.
   * E. Validate that the new {@link JobModel} contains 100 {@link SystemStreamPartition}'s.
   */
  @Test
  public void testShouldGenerateJobModelOnPartitionCountChange() throws Exception {
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create StreamApplication from configuration.
    CountDownLatch kafkaEventsConsumedLatch1 = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
            TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch1,
            applicationConfig1), applicationConfig1);

    executeRun(appRunner1, applicationConfig1);
    processedMessagesLatch1.await();

    String jobModelVersion = zkUtils.getJobModelVersion();
    JobModel jobModel = JobModelUtil.readJobModel(jobModelVersion, zkMetadataStore);
    List<SystemStreamPartition> ssps = getSystemStreamPartitions(jobModel);

    // Validate that the input partition count is 5 in the JobModel.
    Assert.assertEquals(5, ssps.size());

    // Increase the partition count of input kafka topic to 100.
    increasePartitionsTo(inputKafkaTopic1, 100);

    long jobModelWaitTimeInMillis = 10;
    while (Objects.equals(zkUtils.getJobModelVersion(), jobModelVersion)) {
      LOGGER.info("Waiting for new jobModel to be published");
      Thread.sleep(jobModelWaitTimeInMillis);
      jobModelWaitTimeInMillis = jobModelWaitTimeInMillis * 2;
    }

    String newJobModelVersion = zkUtils.getJobModelVersion();
    JobModel newJobModel = JobModelUtil.readJobModel(newJobModelVersion, zkMetadataStore);
    ssps = getSystemStreamPartitions(newJobModel);

    // Validate that the input partition count is 100 in the new JobModel.
    Assert.assertEquals(100, ssps.size());

    // Validate that configuration is stored in coordinator stream.
    MapConfig config = getConfigFromCoordinatorStream(applicationConfig1);

    // Execution plan and serialized DAG of a samza job is stored in the config of coordinator stream. Thus, direct equals comparison between
    // the application configuration and the coordinator config will fail. Iterating through the entire configuration bag and verify that expected
    // configuration is present in the coordinator configuration.
    for (Map.Entry<String, String> entry : applicationConfig1.entrySet()) {
      Assert.assertTrue(config.containsKey(entry.getKey()));
    }
  }

  private MapConfig getConfigFromCoordinatorStream(Config config) {
    MetadataStoreFactory metadataStoreFactory =
        ReflectionUtil.getObj(getClass().getClassLoader(), new JobConfig(config).getMetadataStoreFactory(),
            MetadataStoreFactory.class);
    MetadataStore metadataStore = metadataStoreFactory.getMetadataStore("set-config", config, new MetricsRegistryMap());
    metadataStore.init();
    Map<String, String> configMap = new HashMap<>();
    CoordinatorStreamValueSerde jsonSerde = new CoordinatorStreamValueSerde("set-config");
    metadataStore.all().forEach((key, value) -> {
        CoordinatorStreamStore.CoordinatorMessageKey coordinatorMessageKey = CoordinatorStreamStore.deserializeCoordinatorMessageKeyFromJson(key);
        String deserializedValue = jsonSerde.fromBytes(value);
        configMap.put(coordinatorMessageKey.getKey(), deserializedValue);
      });
    return new MapConfig(configMap);
  }

  /**
   * A. Create a input kafka topic with partition count set to 32.
   * B. Create and launch a stateful samza application which consumes events from the input kafka topic.
   * C. Validate that the {@link JobModel} contains 32 {@link SystemStreamPartition}'s.
   * D. Increase the partition count of the input kafka topic to 64.
   * E. Validate that the new {@link JobModel} contains 64 {@link SystemStreamPartition}'s and the input
   * SystemStreamPartitions are mapped to the correct task.
   */
  @Test
  public void testStatefulSamzaApplicationShouldRedistributeInputPartitionsToCorrectTasksWhenAInputStreamIsExpanded() throws Exception {
    // Setup input topics.
    String statefulInputKafkaTopic = String.format("test-input-topic-%s", UUID.randomUUID().toString());
    createTopic(statefulInputKafkaTopic, 32, 1);

    // Generate configuration for the test.
    Map<String, String> configMap = buildStreamApplicationConfigMap(testStreamAppName, testStreamAppId);
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[0]);
    Config applicationConfig1 = new ApplicationConfig(new MapConfig(configMap));

    publishKafkaEvents(statefulInputKafkaTopic, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create StreamApplication from configuration.
    CountDownLatch kafkaEventsConsumedLatch1 = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
            TEST_SYSTEM, ImmutableList.of(statefulInputKafkaTopic), outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch1,
            applicationConfig1), applicationConfig1);

    executeRun(appRunner1, applicationConfig1);
    processedMessagesLatch1.await();

    // Generate the correct task assignments before the input stream expansion.
    Map<TaskName, Set<SystemStreamPartition>> expectedTaskAssignments = new HashMap<>();
    for (int partition = 0; partition < 32; ++partition) {
      TaskName taskName = new TaskName(String.format("Partition %d", partition));
      SystemStreamPartition systemStreamPartition = new SystemStreamPartition(TEST_SYSTEM, statefulInputKafkaTopic, new Partition(partition));
      expectedTaskAssignments.put(taskName, ImmutableSet.of(systemStreamPartition));
    }

    // Read the latest JobModel for validation.
    String jobModelVersion = zkUtils.getJobModelVersion();
    JobModel jobModel = JobModelUtil.readJobModel(jobModelVersion, zkMetadataStore);
    List<SystemStreamPartition> ssps = getSystemStreamPartitions(jobModel);

    // Validate that the input partition count is 32 in the JobModel.
    Assert.assertEquals(32, ssps.size());

    // Validate that the new JobModel has the expected task assignments before the input stream expansion.
    Map<TaskName, Set<SystemStreamPartition>> actualTaskAssignments = getTaskAssignments(jobModel);
    Assert.assertEquals(expectedTaskAssignments, actualTaskAssignments);

    // Increase the partition count of the input kafka topic to 64.
    increasePartitionsTo(statefulInputKafkaTopic, 64);

    // Wait for the JobModel version to change due to the increase in the input partition count.
    long jobModelWaitTimeInMillis = 10;
    while (true) {
      LOGGER.info("Waiting for new jobModel to be published");
      jobModelVersion = zkUtils.getJobModelVersion();
      jobModel = JobModelUtil.readJobModel(jobModelVersion, zkMetadataStore);
      ssps = getSystemStreamPartitions(jobModel);

      if (ssps.size() == 64) {
        break;
      }
      Thread.sleep(jobModelWaitTimeInMillis);
    }

    // Validate that the input partition count is 64 in the new JobModel.
    Assert.assertEquals(64, ssps.size());

    // Generate the correct task assignments after the input stream expansion.
    expectedTaskAssignments = new HashMap<>();
    for (int partition = 0; partition < 32; ++partition) {
      TaskName taskName = new TaskName(String.format("Partition %d", partition));
      SystemStreamPartition ssp = new SystemStreamPartition(TEST_SYSTEM, statefulInputKafkaTopic, new Partition(partition));
      SystemStreamPartition expandedSSP = new SystemStreamPartition(TEST_SYSTEM, statefulInputKafkaTopic, new Partition(partition + 32));
      expectedTaskAssignments.put(taskName, ImmutableSet.of(ssp, expandedSSP));
    }

    // Validate that the new JobModel has the expected task assignments.
    actualTaskAssignments = getTaskAssignments(jobModel);

    for (Map.Entry<TaskName, Set<SystemStreamPartition>> entry : expectedTaskAssignments.entrySet()) {
      TaskName taskName = entry.getKey();
      Set<SystemStreamPartition> expectedSSPs = entry.getValue();
      Assert.assertTrue(actualTaskAssignments.containsKey(taskName));
      Set<SystemStreamPartition> actualSSPs = actualTaskAssignments.get(taskName);
      Assert.assertEquals(actualSSPs, expectedSSPs);
    }

    Assert.assertEquals(expectedTaskAssignments, actualTaskAssignments);
    Assert.assertEquals(32, jobModel.maxChangeLogStreamPartitions);
  }

  /**
   * A. Create a input kafka topic: T1 with partition count set to 32.
   * B. Create a input kafka topic: T2 with partition count set to 32.
   * C. Create and launch a stateful samza application which consumes events from the kafka topics: T1 and T2.
   * D. Validate that the {@link JobModel} contains 32 {@link SystemStreamPartition}'s of T1 and 32 {@link SystemStreamPartition}'s of T2.
   * E. Increase the partition count of the input kafka topic: T1 to 64.
   * F. Increase the partition count of the input kafka topic: T2 to 64.
   * G. Validate that the new {@link JobModel} contains 64 {@link SystemStreamPartition}'s of T1, T2 and the input
   * SystemStreamPartitions are mapped to the correct task.
   */
  @Test
  public void testStatefulSamzaApplicationShouldRedistributeInputPartitionsToCorrectTasksWhenMultipleInputStreamsAreExpanded() throws Exception {
    // Setup the two input kafka topics.
    String statefulInputKafkaTopic1 = String.format("test-input-topic-%s", UUID.randomUUID().toString());
    String statefulInputKafkaTopic2 = String.format("test-input-topic-%s", UUID.randomUUID().toString());
    createTopic(statefulInputKafkaTopic1, 32, 1);
    createTopic(statefulInputKafkaTopic2, 32, 1);

    // Generate configuration for the test.
    Map<String, String> configMap = buildStreamApplicationConfigMap(testStreamAppName, testStreamAppId);
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[0]);
    Config applicationConfig1 = new ApplicationConfig(new MapConfig(configMap));

    // Publish events into the input kafka topics.
    publishKafkaEvents(statefulInputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);
    publishKafkaEvents(statefulInputKafkaTopic2, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create and launch the StreamApplication from configuration.
    CountDownLatch kafkaEventsConsumedLatch1 = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(statefulInputKafkaTopic1, statefulInputKafkaTopic2), outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch1,
        applicationConfig1), applicationConfig1);

    executeRun(appRunner1, applicationConfig1);
    processedMessagesLatch1.await();
    kafkaEventsConsumedLatch1.await();

    // Generate the correct task assignments before the input stream expansion.
    Map<TaskName, Set<SystemStreamPartition>> expectedTaskAssignments = new HashMap<>();
    for (int partition = 0; partition < 32; ++partition) {
      TaskName taskName = new TaskName(String.format("Partition %d", partition));
      SystemStreamPartition systemStreamPartition1 = new SystemStreamPartition(TEST_SYSTEM, statefulInputKafkaTopic1, new Partition(partition));
      SystemStreamPartition systemStreamPartition2 = new SystemStreamPartition(TEST_SYSTEM, statefulInputKafkaTopic2, new Partition(partition));
      expectedTaskAssignments.put(taskName, ImmutableSet.of(systemStreamPartition1, systemStreamPartition2));
    }

    // Read the latest JobModel for validation.
    String jobModelVersion = zkUtils.getJobModelVersion();
    JobModel jobModel = JobModelUtil.readJobModel(jobModelVersion, zkMetadataStore);
    List<SystemStreamPartition> ssps = getSystemStreamPartitions(jobModel);

    // Validate that the input 64 partitions are present in JobModel.
    Assert.assertEquals(64, ssps.size());

    // Validate that the new JobModel has the expected task assignments before the input stream expansion.
    Map<TaskName, Set<SystemStreamPartition>> actualTaskAssignments = getTaskAssignments(jobModel);
    Assert.assertEquals(expectedTaskAssignments, actualTaskAssignments);

    // Increase the partition count of the input kafka topic1 to 64.
    increasePartitionsTo(statefulInputKafkaTopic1, 64);

    // Increase the partition count of the input kafka topic2 to 64.
    increasePartitionsTo(statefulInputKafkaTopic2, 64);

    // Wait for the JobModel version to change due to the increase in the input partition count.
    long jobModelWaitTimeInMillis = 10;
    while (true) {
      LOGGER.info("Waiting for new jobModel to be published");
      jobModelVersion = zkUtils.getJobModelVersion();
      jobModel = JobModelUtil.readJobModel(jobModelVersion, zkMetadataStore);
      ssps = getSystemStreamPartitions(jobModel);

      if (ssps.size() == 128) {
        break;
      }

      Thread.sleep(jobModelWaitTimeInMillis);
    }

    // Validate that the input partition count is 128 in the new JobModel.
    Assert.assertEquals(128, ssps.size());

    // Generate the correct task assignments after the input stream expansion.
    expectedTaskAssignments = new HashMap<>();
    for (int partition = 0; partition < 32; ++partition) {
      TaskName taskName = new TaskName(String.format("Partition %d", partition));
      SystemStreamPartition ssp1 = new SystemStreamPartition(TEST_SYSTEM, statefulInputKafkaTopic1, new Partition(partition));
      SystemStreamPartition expandedSSP1 = new SystemStreamPartition(TEST_SYSTEM, statefulInputKafkaTopic1, new Partition(partition + 32));
      SystemStreamPartition ssp2 = new SystemStreamPartition(TEST_SYSTEM, statefulInputKafkaTopic2, new Partition(partition));
      SystemStreamPartition expandedSSP2 = new SystemStreamPartition(TEST_SYSTEM, statefulInputKafkaTopic2, new Partition(partition + 32));
      expectedTaskAssignments.put(taskName, ImmutableSet.of(ssp1, expandedSSP1, ssp2, expandedSSP2));
    }

    // Validate that the new JobModel has the expected task assignments.
    actualTaskAssignments = getTaskAssignments(jobModel);
    Assert.assertEquals(expectedTaskAssignments, actualTaskAssignments);
    Assert.assertEquals(32, jobModel.maxChangeLogStreamPartitions);
  }

  @Test
  public void testApplicationShutdownShouldBeIndependentOfPerMessageProcessingTime() throws Exception {
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create a TaskApplication with only one task per container.
    // The task does not invokes taskCallback.complete for any of the dispatched message.
    CountDownLatch shutdownLatch = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);

    TaskApplication taskApplication = new TestTaskApplication(TEST_SYSTEM, inputKafkaTopic1, outputKafkaTopic, processedMessagesLatch1, shutdownLatch, null);
    MapConfig taskApplicationConfig = new MapConfig(ImmutableList.of(applicationConfig1,
        ImmutableMap.of(TaskConfig.MAX_CONCURRENCY, "1", JobConfig.SSP_GROUPER_FACTORY, "org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory")));
    ApplicationRunner appRunner = ApplicationRunners.getApplicationRunner(taskApplication, taskApplicationConfig);

    // Run the application.
    executeRun(appRunner, applicationConfig1);

    // Wait for the task to receive at least one dispatched message.
    processedMessagesLatch1.await();

    // Kill the application when none of the dispatched messages is acknowledged as completed by the task.
    appRunner.kill();
    appRunner.waitForFinish();

    // Expect the shutdown latch to be triggered.
    shutdownLatch.await();

    // Assert that the shutdown was successful.
    Assert.assertEquals(ApplicationStatus.SuccessfulFinish, appRunner.status());
  }

  /**
   * Test if two processors coming up at the same time agree on a single runid
   * 1. bring up two processors
   * 2. wait till they start consuimg messages
   * 3. check if first processor run.id matches that of second processor
   */
  @Test
  public void testAgreeingOnSameRunIdForBatch() throws InterruptedException {
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    Map<String, String> configMap =
        buildStreamApplicationConfigMap(testStreamAppName, testStreamAppId, true);
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[0]);
    applicationConfig1 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[1]);
    applicationConfig2 = new ApplicationConfig(new MapConfig(configMap));


    // Create StreamApplication from configuration.
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, null, null,
        applicationConfig1), applicationConfig1);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch2, null, null,
        applicationConfig2), applicationConfig2);

    executeRun(appRunner1, applicationConfig1);
    executeRun(appRunner2, applicationConfig2);

    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // At this stage, both the processors are running.
    // check if their runId matches

    LocalApplicationRunner localApplicationRunner1 = (LocalApplicationRunner) appRunner1;
    LocalApplicationRunner localApplicationRunner2 = (LocalApplicationRunner) appRunner2;

    assertEquals("RunId of the two processors does not match", localApplicationRunner2.getRunId(), localApplicationRunner1.getRunId());

    appRunner1.kill();
    appRunner1.waitForFinish();
    appRunner2.kill();
    appRunner2.waitForFinish();
  }


  /**
   * Test if a new processors joining an existing qurorum get the same runid
   * 1. bring up two processors
   * 2. wait till they start consuming messages
   * 3. bring up a third processor
   * 4. wait till third processor starts consuming messsages
   * 5. check if third processor run.id matches that of first twp
   */
  @Test
  public void testNewProcessorGetsSameRunIdForBatch() throws InterruptedException {
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    Map<String, String> configMap =
        buildStreamApplicationConfigMap(testStreamAppName, testStreamAppId, true);
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[0]);
    applicationConfig1 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[1]);
    applicationConfig2 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[2]);
    applicationConfig3 = new ApplicationConfig(new MapConfig(configMap));

    // Create StreamApplication from configuration.
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, null, null,
        applicationConfig1), applicationConfig1);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch2, null, null,
        applicationConfig2), applicationConfig2);

    executeRun(appRunner1, applicationConfig1);
    executeRun(appRunner2, applicationConfig2);

    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // At this stage, both the processors are running.
    // check if their runId matches

    LocalApplicationRunner localApplicationRunner1 = (LocalApplicationRunner) appRunner1;
    LocalApplicationRunner localApplicationRunner2 = (LocalApplicationRunner) appRunner2;

    assertEquals("RunId of the two processors does not match", localApplicationRunner2.getRunId(), localApplicationRunner1.getRunId());

    //Bring up a new processsor
    CountDownLatch processedMessagesLatch3 = new CountDownLatch(1);
    ApplicationRunner appRunner3 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch3, null, null,
        applicationConfig3), applicationConfig3);
    executeRun(appRunner3, applicationConfig3);
    processedMessagesLatch3.await();

    // At this stage, the new processor is running.
    // check if new processor's runId matches that of the older processors
    LocalApplicationRunner localApplicationRunner3 = (LocalApplicationRunner) appRunner3;
    assertEquals("RunId of the new processor does not match that of old processor", localApplicationRunner3.getRunId(), localApplicationRunner1.getRunId());


    appRunner1.kill();
    appRunner1.waitForFinish();
    appRunner2.kill();
    appRunner2.waitForFinish();
    appRunner3.kill();
    appRunner3.waitForFinish();
  }


  /**
   * Test one group of processors dying and a new processor coming up generates new run.id
   * 1. bring up two processors
   * 2. wait till they start consuimg messages
   * 3. kill and shutdown neatly both the processors
   * 4. bring up a new processor
   * 5. wait till new processor starts consuming messages
   * 6. check if new processor has new runid different from shutdown processors
   */
  @Test
  public void testAllProcesssorDieNewProcessorGetsNewRunIdForBatch() throws InterruptedException {
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    Map<String, String> configMap =
        buildStreamApplicationConfigMap(testStreamAppName, testStreamAppId, true);
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[0]);
    applicationConfig1 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[1]);
    applicationConfig2 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[2]);
    applicationConfig3 = new ApplicationConfig(new MapConfig(configMap));

    // Create StreamApplication from configuration.
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, null, null,
        applicationConfig1), applicationConfig1);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch2, null, null,
        applicationConfig2), applicationConfig2);

    executeRun(appRunner1, applicationConfig1);
    executeRun(appRunner2, applicationConfig2);

    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // At this stage, both the processors are running.
    // check if their runId matches

    LocalApplicationRunner localApplicationRunner1 = (LocalApplicationRunner) appRunner1;
    LocalApplicationRunner localApplicationRunner2 = (LocalApplicationRunner) appRunner2;

    assertEquals("RunId of the two processors does not match", localApplicationRunner2.getRunId(), localApplicationRunner1.getRunId());

    String oldRunId = localApplicationRunner1.getRunId().get();

    // shut down both the processors
    appRunner1.kill();
    appRunner1.waitForFinish();
    appRunner2.kill();
    appRunner2.waitForFinish();

    //Bring up a new processsor
    CountDownLatch processedMessagesLatch3 = new CountDownLatch(1);
    ApplicationRunner appRunner3 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch3, null, null,
        applicationConfig3), applicationConfig3);
    executeRun(appRunner3, applicationConfig3);
    processedMessagesLatch3.await();

    // At this stage, the new processor is running.
    // check if new processor's runId matches that of the older processors
    LocalApplicationRunner localApplicationRunner3 = (LocalApplicationRunner) appRunner3;

    assertNotEquals("RunId of the new processor same as that of old stopped processors", oldRunId, localApplicationRunner3.getRunId());

    appRunner3.kill();
    appRunner3.waitForFinish();
  }


  /**
   * Test if first processor dying changes the runid for new processors joining
   * 1. bring up two processors
   * 2. wait till they start consuimg messages
   * 3. kill and shutdown first processor
   * 4. bring up a new processor
   * 5. wait till new processor starts consuming messages
   * 6. check if new processor gets same run.id
   */
  @Test
  public void testFirstProcessorDiesButSameRunIdForBatch() throws InterruptedException {
    publishKafkaEvents(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    Map<String, String> configMap =
        buildStreamApplicationConfigMap(testStreamAppName, testStreamAppId, true);
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[0]);
    applicationConfig1 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[1]);
    applicationConfig2 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID, PROCESSOR_IDS[2]);
    applicationConfig3 = new ApplicationConfig(new MapConfig(configMap));

    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    ApplicationRunner appRunner1 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch1, null, null,
        applicationConfig1), applicationConfig1);
    executeRun(appRunner1, applicationConfig1);

    // firt processor is up and running
    processedMessagesLatch1.await();

    LocalApplicationRunner localApplicationRunner1 = (LocalApplicationRunner) appRunner1;

    // bring up second processor
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);
    ApplicationRunner appRunner2 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch2, null, null,
        applicationConfig2), applicationConfig2);
    executeRun(appRunner2, applicationConfig2);

    // second processor is up and running
    processedMessagesLatch2.await();
    LocalApplicationRunner localApplicationRunner2 = (LocalApplicationRunner) appRunner2;

    assertEquals("RunId of the two processors does not match", localApplicationRunner2.getRunId(), localApplicationRunner1.getRunId());

    // shut down first processor
    appRunner1.kill();
    appRunner1.waitForFinish();

    //Bring up a new processsor
    CountDownLatch processedMessagesLatch3 = new CountDownLatch(1);
    ApplicationRunner appRunner3 = ApplicationRunners.getApplicationRunner(TestStreamApplication.getInstance(
        TEST_SYSTEM, ImmutableList.of(inputKafkaTopic1), outputKafkaTopic, processedMessagesLatch3, null, null,
        applicationConfig3), applicationConfig3);
    executeRun(appRunner3, applicationConfig3);
    processedMessagesLatch3.await();

    // At this stage, the new processor is running.
    // check if new processor runid matches the old ones
    LocalApplicationRunner localApplicationRunner3 = (LocalApplicationRunner) appRunner3;
    assertEquals("RunId of the new processor is not the same as that of earlier processors", localApplicationRunner2.getRunId(), localApplicationRunner3.getRunId());


    appRunner2.kill();
    appRunner2.waitForFinish();
    appRunner3.kill();
    appRunner3.waitForFinish();
  }

  private void writeStartpoints(StartpointManager startpointManager, String streamName, Integer partitionCount, Startpoint startpoint) {
    for (int p = 0; p < partitionCount; p++) {
      startpointManager.writeStartpoint(new SystemStreamPartition(TEST_SYSTEM, streamName, new Partition(p)), startpoint);
    }
  }

  @Test
  public void testStartpoints() throws InterruptedException {
    TreeMap<Integer, RecordMetadata> sentEvents1 =
        publishKafkaEventsWithDelayPerEvent(inputKafkaTopic1, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0], Duration.ofMillis(2));
    TreeMap<Integer, RecordMetadata> sentEvents2 =
        publishKafkaEventsWithDelayPerEvent(inputKafkaTopic2, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[1], Duration.ofMillis(2));
    TreeMap<Integer, RecordMetadata> sentEvents3 =
        publishKafkaEventsWithDelayPerEvent(inputKafkaTopic3, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[2], Duration.ofMillis(2));
    TreeMap<Integer, RecordMetadata> sentEvents4 =
        publishKafkaEventsWithDelayPerEvent(inputKafkaTopic4, 0, NUM_KAFKA_EVENTS, PROCESSOR_IDS[3], Duration.ofMillis(2));

    ConcurrentHashMap<String, IncomingMessageEnvelope> recvEventsInputStartpointSpecific = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, IncomingMessageEnvelope> recvEventsInputStartpointTimestamp = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, IncomingMessageEnvelope> recvEventsInputStartpointOldest = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, IncomingMessageEnvelope> recvEventsInputStartpointUpcoming = new ConcurrentHashMap<>();

    CoordinatorStreamStore coordinatorStreamStore = createCoordinatorStreamStore(applicationConfig1);
    coordinatorStreamStore.init();
    StartpointManager startpointManager = new StartpointManager(coordinatorStreamStore);
    startpointManager.start();

    StartpointSpecific startpointSpecific = new StartpointSpecific(String.valueOf(sentEvents1.get(100).offset()));
    writeStartpoints(startpointManager, inputKafkaTopic1, 5, startpointSpecific);

    StartpointTimestamp startpointTimestamp = new StartpointTimestamp(sentEvents2.get(150).timestamp());
    writeStartpoints(startpointManager, inputKafkaTopic2, 5, startpointTimestamp);

    StartpointOldest startpointOldest = new StartpointOldest();
    writeStartpoints(startpointManager, inputKafkaTopic3, 5, startpointOldest);

    StartpointUpcoming startpointUpcoming = new StartpointUpcoming();
    writeStartpoints(startpointManager, inputKafkaTopic4, 5, startpointUpcoming);

    startpointManager.stop();
    coordinatorStreamStore.close();

    TestTaskApplication.TaskApplicationCallback processedCallback = (IncomingMessageEnvelope ime, TaskCallback callback) -> {
      try {
        String streamName = ime.getSystemStreamPartition().getStream();
        TestStreamApplication.TestKafkaEvent testKafkaEvent =
            TestStreamApplication.TestKafkaEvent.fromString((String) ime.getMessage());
        String eventIndex = testKafkaEvent.getEventData();
        if (inputKafkaTopic1.equals(streamName)) {
          recvEventsInputStartpointSpecific.put(eventIndex, ime);
        } else if (inputKafkaTopic2.equals(streamName)) {
          recvEventsInputStartpointTimestamp.put(eventIndex, ime);
        } else if (inputKafkaTopic3.equals(streamName)) {
          recvEventsInputStartpointOldest.put(eventIndex, ime);
        } else if (inputKafkaTopic4.equals(streamName)) {
          recvEventsInputStartpointUpcoming.put(eventIndex, ime);
        } else {
          throw new RuntimeException("Unexpected input stream: " + streamName);
        }
        callback.complete();
      } catch (Exception ex) {
        callback.failure(ex);
      }
    };

    // Create StreamApplication from configuration.
    CountDownLatch processedMessagesLatchStartpointSpecific = new CountDownLatch(100); // Just fetch a few messages
    CountDownLatch processedMessagesLatchStartpointTimestamp = new CountDownLatch(100); // Just fetch a few messages
    CountDownLatch processedMessagesLatchStartpointOldest = new CountDownLatch(NUM_KAFKA_EVENTS); // Just fetch a few messages
    CountDownLatch processedMessagesLatchStartpointUpcoming = new CountDownLatch(5); // Just fetch a few messages

    CountDownLatch shutdownLatchStartpointSpecific = new CountDownLatch(1);
    CountDownLatch shutdownLatchStartpointTimestamp = new CountDownLatch(1);
    CountDownLatch shutdownLatchStartpointOldest = new CountDownLatch(1);
    CountDownLatch shutdownLatchStartpointUpcoming = new CountDownLatch(1);

    TestTaskApplication testTaskApplicationStartpointSpecific =
        new TestTaskApplication(TEST_SYSTEM, inputKafkaTopic1, outputKafkaTopic, processedMessagesLatchStartpointSpecific,
            shutdownLatchStartpointSpecific, processedCallback);
    TestTaskApplication testTaskApplicationStartpointTimestamp =
        new TestTaskApplication(TEST_SYSTEM, inputKafkaTopic2, outputKafkaTopic, processedMessagesLatchStartpointTimestamp,
            shutdownLatchStartpointTimestamp, processedCallback);
    TestTaskApplication testTaskApplicationStartpointOldest =
        new TestTaskApplication(TEST_SYSTEM, inputKafkaTopic3, outputKafkaTopic, processedMessagesLatchStartpointOldest,
            shutdownLatchStartpointOldest, processedCallback);
    TestTaskApplication testTaskApplicationStartpointUpcoming =
        new TestTaskApplication(TEST_SYSTEM, inputKafkaTopic4, outputKafkaTopic, processedMessagesLatchStartpointUpcoming,
            shutdownLatchStartpointUpcoming, processedCallback);

    // Startpoint specific
    ApplicationRunner appRunner = ApplicationRunners.getApplicationRunner(testTaskApplicationStartpointSpecific, applicationConfig1);
    executeRun(appRunner, applicationConfig1);

    assertTrue(processedMessagesLatchStartpointSpecific.await(1, TimeUnit.MINUTES));

    appRunner.kill();
    appRunner.waitForFinish();

    assertTrue(shutdownLatchStartpointSpecific.await(1, TimeUnit.MINUTES));

    Integer startpointSpecificOffset = Integer.valueOf(startpointSpecific.getSpecificOffset());
    for (IncomingMessageEnvelope ime : recvEventsInputStartpointSpecific.values()) {
      Integer eventOffset = Integer.valueOf(ime.getOffset());
      String assertMsg = String.format("Expecting message offset: %d >= Startpoint specific offset: %d",
          eventOffset, startpointSpecificOffset);
      assertTrue(assertMsg, eventOffset >= startpointSpecificOffset);
    }

    // Startpoint timestamp
    appRunner = ApplicationRunners.getApplicationRunner(testTaskApplicationStartpointTimestamp, applicationConfig2);

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

    // Startpoint oldest
    appRunner = ApplicationRunners.getApplicationRunner(testTaskApplicationStartpointOldest, applicationConfig3);
    executeRun(appRunner, applicationConfig3);

    assertTrue(processedMessagesLatchStartpointOldest.await(1, TimeUnit.MINUTES));

    appRunner.kill();
    appRunner.waitForFinish();

    assertTrue(shutdownLatchStartpointOldest.await(1, TimeUnit.MINUTES));

    assertEquals("Expecting to have processed all the events", NUM_KAFKA_EVENTS, recvEventsInputStartpointOldest.size());

    // Startpoint upcoming
    appRunner = ApplicationRunners.getApplicationRunner(testTaskApplicationStartpointUpcoming, applicationConfig4);
    executeRun(appRunner, applicationConfig4);

    assertFalse("Expecting to timeout and not process any old messages.", processedMessagesLatchStartpointUpcoming.await(15, TimeUnit.SECONDS));
    assertEquals("Expecting not to process any old messages.", 0, recvEventsInputStartpointUpcoming.size());

    appRunner.kill();
    appRunner.waitForFinish();

    assertTrue(shutdownLatchStartpointUpcoming.await(1, TimeUnit.MINUTES));
  }

  /**
   * Computes the task to partition assignment of the {@param JobModel}.
   * @param jobModel the jobModel to compute task to partition assignment for.
   * @return the computed task to partition assignments of the {@param JobModel}.
   */
  private static Map<TaskName, Set<SystemStreamPartition>> getTaskAssignments(JobModel jobModel) {
    Map<TaskName, Set<SystemStreamPartition>> taskAssignments = new HashMap<>();
    for (Map.Entry<String, ContainerModel> entry : jobModel.getContainers().entrySet()) {
      Map<TaskName, TaskModel> tasks = entry.getValue().getTasks();
      for (TaskModel taskModel : tasks.values()) {
        if (!taskAssignments.containsKey(taskModel.getTaskName())) {
          taskAssignments.put(taskModel.getTaskName(), new HashSet<>());
        }
        if (taskModel.getTaskMode() == TaskMode.Active) {
          taskAssignments.get(taskModel.getTaskName()).addAll(taskModel.getSystemStreamPartitions());
        }
      }
    }
    return taskAssignments;
  }

  private static List<SystemStreamPartition> getSystemStreamPartitions(JobModel jobModel) {
    List<SystemStreamPartition> ssps = new ArrayList<>();
    jobModel.getContainers().forEach((containerName, containerModel) -> {
        containerModel.getTasks().forEach((taskName, taskModel) -> ssps.addAll(taskModel.getSystemStreamPartitions()));
      });
    return ssps;
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
}
