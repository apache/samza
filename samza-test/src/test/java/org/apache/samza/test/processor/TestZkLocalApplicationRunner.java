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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import kafka.admin.AdminUtils;
import kafka.server.KafkaServer;
import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.test.StandaloneIntegrationTestHarness;
import org.apache.samza.test.StandaloneTestUtils;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.apache.samza.zk.ZkKeyBuilder;
import org.apache.samza.zk.ZkUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import scala.collection.JavaConverters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Integration tests for {@link org.apache.samza.runtime.LocalApplicationRunner}.
 *
 * Brings up embedded ZooKeeper, Kafka broker and launches multiple {@link StreamApplication} through
 * {@link org.apache.samza.runtime.LocalApplicationRunner} to verify the guarantees made in stand alone execution environment.
 */
public class TestZkLocalApplicationRunner extends StandaloneIntegrationTestHarness {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestZkLocalApplicationRunner.class);

  private static final int NUM_KAFKA_EVENTS = 5;
  private static final int ZK_CONNECTION_TIMEOUT_MS = 100;
  private static final String TEST_SYSTEM = "TestSystemName";
  private static final String TEST_SSP_GROUPER_FACTORY = "org.apache.samza.container.grouper.stream.GroupByPartitionFactory";
  private static final String TEST_TASK_GROUPER_FACTORY = "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory";
  private static final String TEST_JOB_COORDINATOR_FACTORY = "org.apache.samza.zk.ZkJobCoordinatorFactory";
  private static final String TEST_SYSTEM_FACTORY = "org.apache.samza.system.kafka.KafkaSystemFactory";
  private static final String TASK_SHUTDOWN_MS = "3000";
  private static final String JOB_DEBOUNCE_TIME_MS = "1000";
  private static final String[] PROCESSOR_IDS = new String[] {"0000000000", "0000000001", "0000000002"};

  private String inputKafkaTopic;
  private String outputKafkaTopic;
  private ZkUtils zkUtils;
  private ApplicationConfig applicationConfig1;
  private ApplicationConfig applicationConfig2;
  private ApplicationConfig applicationConfig3;
  private String testStreamAppName;
  private String testStreamAppId;

  @Rule
  public Timeout testTimeOutInMillis = new Timeout(120000);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Override
  public void setUp() {
    super.setUp();
    String uniqueTestId = UUID.randomUUID().toString();
    testStreamAppName = String.format("test-app-name-%s", uniqueTestId);
    testStreamAppId = String.format("test-app-id-%s", uniqueTestId);
    inputKafkaTopic = String.format("test-input-topic-%s", uniqueTestId);
    outputKafkaTopic = String.format("test-output-topic-%s", uniqueTestId);

    // Set up stream application config map with the given testStreamAppName, testStreamAppId and test kafka system
    // TODO: processorId should typically come up from a processorID generator as processor.id will be deprecated in 0.14.0+
    Map<String, String> configMap =
        buildStreamApplicationConfigMap(TEST_SYSTEM, inputKafkaTopic, testStreamAppName, testStreamAppId);
    configMap.put(JobConfig.PROCESSOR_ID(), PROCESSOR_IDS[0]);
    applicationConfig1 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID(), PROCESSOR_IDS[1]);
    applicationConfig2 = new ApplicationConfig(new MapConfig(configMap));
    configMap.put(JobConfig.PROCESSOR_ID(), PROCESSOR_IDS[2]);
    applicationConfig3 = new ApplicationConfig(new MapConfig(configMap));

    ZkClient zkClient = new ZkClient(zkConnect());
    ZkKeyBuilder zkKeyBuilder = new ZkKeyBuilder(ZkJobCoordinatorFactory.getJobCoordinationZkPath(applicationConfig1));
    zkUtils = new ZkUtils(zkKeyBuilder, zkClient, ZK_CONNECTION_TIMEOUT_MS, new NoOpMetricsRegistry());
    zkUtils.connect();

    for (String kafkaTopic : ImmutableList.of(inputKafkaTopic, outputKafkaTopic)) {
      LOGGER.info("Creating kafka topic: {}.", kafkaTopic);
      TestUtils.createTopic(zkUtils(), kafkaTopic, 5, 1, servers(), new Properties());
    }
  }

  @Override
  public void tearDown() {
    SharedContextFactories.clearAll();
    if (zookeeper().zookeeper().isRunning()) {
      for (String kafkaTopic : ImmutableList.of(inputKafkaTopic, outputKafkaTopic)) {
        LOGGER.info("Deleting kafka topic: {}.", kafkaTopic);
        AdminUtils.deleteTopic(zkUtils(), kafkaTopic);
      }
      zkUtils.close();
      super.tearDown();
    }
  }

  private void publishKafkaEvents(String topic, int numEvents, String streamProcessorId) {
    KafkaProducer producer = getKafkaProducer();
    for (int eventIndex = 0; eventIndex < numEvents; eventIndex++) {
      try {
        LOGGER.info("Publish kafka event with index : {} for stream processor: {}.", eventIndex, streamProcessorId);
        producer.send(new ProducerRecord(topic, new TestStreamApplication.TestKafkaEvent(streamProcessorId, String.valueOf(eventIndex)).toString().getBytes()));
      } catch (Exception  e) {
        LOGGER.error("Publishing to kafka topic: {} resulted in exception: {}.", new Object[]{topic, e});
        throw new SamzaException(e);
      }
    }
  }

  private Map<String, String> buildStreamApplicationConfigMap(String systemName, String inputTopic,
                                                              String appName, String appId) {
    Map<String, String> samzaContainerConfig = ImmutableMap.<String, String>builder()
        .put(TaskConfig.INPUT_STREAMS(), inputTopic)
        .put(JobConfig.JOB_DEFAULT_SYSTEM(), systemName)
        .put(TaskConfig.IGNORED_EXCEPTIONS(), "*")
        .put(ZkConfig.ZK_CONNECT, zkConnect())
        .put(JobConfig.SSP_GROUPER_FACTORY(), TEST_SSP_GROUPER_FACTORY)
        .put(TaskConfig.GROUPER_FACTORY(), TEST_TASK_GROUPER_FACTORY)
        .put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, TEST_JOB_COORDINATOR_FACTORY)
        .put(ApplicationConfig.APP_NAME, appName)
        .put(ApplicationConfig.APP_ID, appId)
        .put("app.runner.class", "org.apache.samza.runtime.LocalApplicationRunner")
        .put(String.format("systems.%s.samza.factory", systemName), TEST_SYSTEM_FACTORY)
        .put(JobConfig.JOB_NAME(), appName)
        .put(JobConfig.JOB_ID(), appId)
        .put(TaskConfigJava.TASK_SHUTDOWN_MS, TASK_SHUTDOWN_MS)
        .put(JobConfig.JOB_DEBOUNCE_TIME_MS(), JOB_DEBOUNCE_TIME_MS)
        .build();
    Map<String, String> applicationConfig = Maps.newHashMap(samzaContainerConfig);
    applicationConfig.putAll(StandaloneTestUtils.getKafkaSystemConfigs(systemName, bootstrapServers(), zkConnect(), null, StandaloneTestUtils.SerdeAlias.STRING, true));
    return applicationConfig;
  }

  @Test
  public void shouldStopNewProcessorsJoiningGroupWhenNumContainersIsGreaterThanNumTasks()
      throws InterruptedException, IOException {
    /**
     * sspGrouper is set to AllSspToSingleTaskGrouperFactory for this test case(All ssp's from input kafka topic are mapped to a single task).
     * Run a stream application(streamApp1) consuming messages from input topic(effectively one container).
     *
     * In the callback triggered by streamApp1 after processing a message, bring up an another stream application(streamApp2).
     *
     * Assertions:
     *           A) JobModel generated before and after the addition of streamApp2 should be equal.
     *           B) Second stream application(streamApp2) should not join the group and process any message.
     */

    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS * 2, PROCESSOR_IDS[0]);

    // Configuration, verification variables
    MapConfig testConfig = new MapConfig(ImmutableMap.of(JobConfig.SSP_GROUPER_FACTORY(), "org.apache.samza.container.grouper.stream.AllSspToSingleTaskGrouperFactory", JobConfig.JOB_DEBOUNCE_TIME_MS(), "10"));
    // Declared as final array to update it from streamApplication callback(Variable should be declared final to access in lambda block).
    final JobModel[] previousJobModel = new JobModel[1];
    final String[] previousJobModelVersion = new String[1];

    CountDownLatch secondProcessorRegistered = new CountDownLatch(1);

    zkUtils.subscribeToProcessorChange((parentPath, currentChilds) -> {
        // When streamApp2 with id: PROCESSOR_IDS[1] is registered, start processing message in streamApp1.
        if (currentChilds.contains(PROCESSOR_IDS[1])) {
          secondProcessorRegistered.countDown();
        }
      });

    // Set up stream app 2.
    CountDownLatch processedMessageLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    Config localTestConfig2 = new MapConfig(applicationConfig2, testConfig);
    StreamApplication
        streamApp2 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessageLatch, null, null, localTestConfig2);

    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS * 2);

    // Set up stream app 1.
    Config localTestConfig1 = new MapConfig(applicationConfig1, testConfig);
    AtomicBoolean hasSecondProcessorJoined = new AtomicBoolean(false);
    CountDownLatch processedLatch = new CountDownLatch(1);
    TestStreamApplication.StreamApplicationCallback callback = m -> {
      if (hasSecondProcessorJoined.compareAndSet(false, true)) {
        processedLatch.countDown();
        try {
          secondProcessorRegistered.await();
        } catch (InterruptedException ie) {
          throw new SamzaException(ie);
        }
      }
    };

    StreamApplication
        streamApp1 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, null, callback, kafkaEventsConsumedLatch, localTestConfig1);
    streamApp1.run();

    processedLatch.await();
    previousJobModelVersion[0] = zkUtils.getJobModelVersion();
    previousJobModel[0] = zkUtils.getJobModel(previousJobModelVersion[0]);
    streamApp2.run();

    kafkaEventsConsumedLatch.await();

    String currentJobModelVersion = zkUtils.getJobModelVersion();
    JobModel updatedJobModel = zkUtils.getJobModel(currentJobModelVersion);

    // JobModelVersion check to verify that leader publishes new jobModel.
    assertTrue(Integer.parseInt(previousJobModelVersion[0]) < Integer.parseInt(currentJobModelVersion));
    // Job model before and after the addition of second stream processor should be the same.
    assertEquals(previousJobModel[0], updatedJobModel);
    // TODO: After SAMZA-1364 add assertion for localApplicationRunner2.status(streamApp)
    // ProcessedMessagesLatch shouldn't have changed. Should retain it's initial value.
    assertEquals(NUM_KAFKA_EVENTS, processedMessageLatch.getCount());

    streamApp1.kill().waitForFinish();
    streamApp2.kill().waitForFinish();

  }

  @Test
  public void shouldReElectLeaderWhenLeaderDies() throws InterruptedException, IOException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic, 2 * NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create stream applications.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(2 * NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch3 = new CountDownLatch(1);

    StreamApplication
        streamApp1 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch, applicationConfig1);
    StreamApplication
        streamApp2 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch, applicationConfig2);
    StreamApplication
        streamApp3 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch3, null, kafkaEventsConsumedLatch, applicationConfig3);

    streamApp1.run();
    streamApp2.run();
    streamApp3.run();

    // Wait until all processors have processed a message.
    processedMessagesLatch1.await();
    processedMessagesLatch2.await();
    processedMessagesLatch3.await();

    // Verifications before killing the leader.
    String jobModelVersion = zkUtils.getJobModelVersion();
    JobModel jobModel = zkUtils.getJobModel(jobModelVersion);
    assertEquals(3, jobModel.getContainers().size());
    assertEquals(Sets.newHashSet("0000000000", "0000000001", "0000000002"), jobModel.getContainers().keySet());
    assertEquals("1", jobModelVersion);

    List<String> processorIdsFromZK = zkUtils.getActiveProcessorsIDs(Arrays.asList(PROCESSOR_IDS));

    assertEquals(3, processorIdsFromZK.size());
    assertEquals(PROCESSOR_IDS[0], processorIdsFromZK.get(0));

    // Kill the leader. Since streamApp1 is the first to join the cluster, it's the leader.
    streamApp1.kill().waitForFinish();
    kafkaEventsConsumedLatch.await();

    // Verifications after killing the leader.
    assertEquals(ApplicationStatus.SuccessfulFinish, streamApp1.status());
    processorIdsFromZK = zkUtils.getActiveProcessorsIDs(ImmutableList.of(PROCESSOR_IDS[1], PROCESSOR_IDS[2]));
    assertEquals(2, processorIdsFromZK.size());
    assertEquals(PROCESSOR_IDS[1], processorIdsFromZK.get(0));
    jobModelVersion = zkUtils.getJobModelVersion();
    assertEquals("2", jobModelVersion);
    jobModel = zkUtils.getJobModel(jobModelVersion);
    assertEquals(Sets.newHashSet("0000000001", "0000000002"), jobModel.getContainers().keySet());
    assertEquals(2, jobModel.getContainers().size());

    streamApp2.kill().waitForFinish();
    streamApp3.kill().waitForFinish();

  }

  @Test
  public void shouldFailWhenNewProcessorJoinsWithSameIdAsExistingProcessor() throws InterruptedException, IOException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create StreamApplications.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    StreamApplication
        streamApp1 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch, applicationConfig1);
    StreamApplication
        streamApp2 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch, applicationConfig2);

    // Run stream applications.
    streamApp1.run();
    streamApp2.run();

    // Wait for message processing to start in both the processors.
    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // Create a stream app with same processor id as SP2 and run it. It should fail.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[2]);
    kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    StreamApplication
        streamApp3 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, null, null, kafkaEventsConsumedLatch, applicationConfig2);
    // Fail when the duplicate processor joins.
    expectedException.expect(SamzaException.class);
    streamApp3.run();

    streamApp1.kill().waitForFinish();
    streamApp2.kill().waitForFinish();
    streamApp3.kill().waitForFinish();

  }

  @Test
  public void testRollingUpgradeOfStreamApplicationsShouldGenerateSameJobModel() throws Exception {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    /**
     * Custom listeners can't be plugged in for transition events(generatingNewJobModel, waitingForProcessors, waitingForBarrierCompletion etc) from zkJobCoordinator. Only possible listeners
     * are for ZkJobCoordinator output(onNewJobModelConfirmed, onNewJobModelAvailable). Increasing DefaultDebounceTime to make sure that streamApplication dies & rejoins before expiry.
     */
    Map<String, String> debounceTimeConfig = ImmutableMap.of(JobConfig.JOB_DEBOUNCE_TIME_MS(), "40000");
    Map<String, String> configMap = buildStreamApplicationConfigMap(TEST_SYSTEM, inputKafkaTopic, testStreamAppName, testStreamAppId);
    configMap.put(JobConfig.JOB_DEBOUNCE_TIME_MS(), "40000");

    configMap.put(JobConfig.PROCESSOR_ID(), PROCESSOR_IDS[0]);
    Config applicationConfig1 = new MapConfig(configMap);

    configMap.put(JobConfig.PROCESSOR_ID(), PROCESSOR_IDS[1]);
    Config applicationConfig2 = new MapConfig(configMap);

    List<TestStreamApplication.TestKafkaEvent> messagesProcessed = new ArrayList<>();
    TestStreamApplication.StreamApplicationCallback streamApplicationCallback = messagesProcessed::add;

    // Create StreamApplication from configuration.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    StreamApplication streamApp1 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, streamApplicationCallback, kafkaEventsConsumedLatch, applicationConfig1);
    StreamApplication streamApp2 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch, applicationConfig2);

    // Run stream application.
    streamApp1.run();
    streamApp2.run();

    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // Read job model before rolling upgrade.
    String jobModelVersion = zkUtils.getJobModelVersion();
    JobModel jobModel = zkUtils.getJobModel(jobModelVersion);

    streamApp1.kill().waitForFinish();

    int lastProcessedMessageId = -1;
    for (TestStreamApplication.TestKafkaEvent message : messagesProcessed) {
      lastProcessedMessageId = Math.max(lastProcessedMessageId, Integer.parseInt(message.getEventData()));
    }
    messagesProcessed.clear();

    processedMessagesLatch1 = new CountDownLatch(1);
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);
    streamApp1 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, streamApplicationCallback, kafkaEventsConsumedLatch, applicationConfig1);
    streamApp1.run();

    processedMessagesLatch1.await();

    // Read new job model after rolling upgrade.
    String newJobModelVersion = zkUtils.getJobModelVersion();
    JobModel newJobModel = zkUtils.getJobModel(newJobModelVersion);

    // This should be continuation of last processed message.
    int nextSeenMessageId = Integer.parseInt(messagesProcessed.get(0).getEventData());
    assertTrue(lastProcessedMessageId <= nextSeenMessageId);
    assertEquals(Integer.parseInt(jobModelVersion) + 1, Integer.parseInt(newJobModelVersion));
    assertEquals(jobModel.getContainers(), newJobModel.getContainers());

    streamApp1.kill().waitForFinish();
    streamApp2.kill().waitForFinish();

  }

  @Test
  public void shouldKillStreamAppWhenZooKeeperDiesBeforeLeaderReElection() throws InterruptedException, IOException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    MapConfig kafkaProducerConfig = new MapConfig(ImmutableMap.of(String.format("systems.%s.producer.%s", TEST_SYSTEM, ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG), "1000"));
    MapConfig applicationRunnerConfig1 = new MapConfig(ImmutableList.of(applicationConfig1, kafkaProducerConfig));
    MapConfig applicationRunnerConfig2 = new MapConfig(ImmutableList.of(applicationConfig2, kafkaProducerConfig));

    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    // Create StreamApplications.
    StreamApplication streamApp1 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, null, null, applicationRunnerConfig1);
    StreamApplication streamApp2 = TestStreamApplication.getInstance(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2, null, null, applicationRunnerConfig2);

    // Run stream applications.
    streamApp1.run();
    streamApp2.run();

    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // Non daemon thread in brokers reconnect repeatedly to zookeeper on failures. Manually shutting them down.
    List<KafkaServer> kafkaServers = JavaConverters.bufferAsJavaListConverter(this.servers()).asJava();
    kafkaServers.forEach(KafkaServer::shutdown);

    zookeeper().shutdown();

    assertEquals(ApplicationStatus.UnsuccessfulFinish, streamApp1.status());
    assertEquals(ApplicationStatus.UnsuccessfulFinish, streamApp2.status());
  }
}
