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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import kafka.admin.AdminUtils;
import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.test.StandaloneIntegrationTestHarness;
import org.apache.samza.test.StandaloneTestUtils;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.apache.samza.zk.ZkKeyBuilder;
import org.apache.samza.zk.ZkUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

/**
 * Integration tests for {@link LocalApplicationRunner}.
 *
 * Brings up embedded ZooKeeper, Kafka broker and launches multiple {@link StreamApplication} through
 * {@link LocalApplicationRunner} to verify the guarantees made in stand alone execution environment.
 */
public class TestZkLocalApplicationRunner extends StandaloneIntegrationTestHarness {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestZkLocalApplicationRunner.class);

  private static final int NUM_KAFKA_EVENTS = 300;
  private static final int ZK_CONNECTION_TIMEOUT_MS = 10000;
  private static final String TEST_SYSTEM = "TestSystemName";
  private static final String TEST_SSP_GROUPER_FACTORY = "org.apache.samza.container.grouper.stream.GroupByPartitionFactory";
  private static final String TEST_TASK_GROUPER_FACTORY = "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory";
  private static final String TEST_JOB_COORDINATOR_FACTORY = "org.apache.samza.zk.ZkJobCoordinatorFactory";
  private static final String TEST_SYSTEM_FACTORY = "org.apache.samza.system.kafka.KafkaSystemFactory";
  private static final String TEST_JOB_NAME = "test-job";
  private static final String[] PROCESSOR_IDS = new String[] {"0000000000", "0000000001", "0000000002"};

  private String inputKafkaTopic;
  private String outputKafkaTopic;
  private ZkUtils zkUtils;
  private ApplicationConfig applicationConfig1;
  private ApplicationConfig applicationConfig2;
  private ApplicationConfig applicationConfig3;
  private LocalApplicationRunner applicationRunner1;
  private LocalApplicationRunner applicationRunner2;
  private LocalApplicationRunner applicationRunner3;

  // Set 90 seconds as max execution time for each test.
  @Rule
  public Timeout testTimeOutInMillis = new Timeout(90000);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Override
  public void setUp() {
    super.setUp();
    String uniqueTestId = UUID.randomUUID().toString();
    String testStreamAppName = String.format("test-app-name-%s", uniqueTestId);
    String testStreamAppId = String.format("test-app-id-%s", uniqueTestId);
    inputKafkaTopic = String.format("test-input-topic-%s", uniqueTestId);
    outputKafkaTopic = String.format("test-output-topic-%s", uniqueTestId);
    ZkClient zkClient = new ZkClient(zkConnect());
    ZkKeyBuilder zkKeyBuilder = new ZkKeyBuilder(String.format("app-%s-%s", testStreamAppName, testStreamAppId));
    zkUtils = new ZkUtils(zkKeyBuilder, zkClient, ZK_CONNECTION_TIMEOUT_MS, new NoOpMetricsRegistry());
    zkUtils.connect();

    // Set up stream application configs with different processorIds and same testStreamAppName, testStreamAppId.
    applicationConfig1 = buildStreamApplicationConfig(TEST_SYSTEM, inputKafkaTopic, PROCESSOR_IDS[0], testStreamAppName, testStreamAppId);
    applicationConfig2 = buildStreamApplicationConfig(TEST_SYSTEM, inputKafkaTopic, PROCESSOR_IDS[1], testStreamAppName, testStreamAppId);
    applicationConfig3 = buildStreamApplicationConfig(TEST_SYSTEM, inputKafkaTopic, PROCESSOR_IDS[2], testStreamAppName, testStreamAppId);

    // Create local application runners.
    applicationRunner1 = new LocalApplicationRunner(applicationConfig1);
    applicationRunner2 = new LocalApplicationRunner(applicationConfig2);
    applicationRunner3 = new LocalApplicationRunner(applicationConfig3);

    for (String kafkaTopic : ImmutableList.of(inputKafkaTopic, outputKafkaTopic)) {
      LOGGER.info("Creating kafka topic: {}.", kafkaTopic);
      TestUtils.createTopic(zkUtils(), kafkaTopic, 5, 1, servers(), new Properties());
    }
  }

  @Override
  public void tearDown() {
    for (String kafkaTopic : ImmutableList.of(inputKafkaTopic, outputKafkaTopic)) {
      LOGGER.info("Deleting kafka topic: {}.", kafkaTopic);
      AdminUtils.deleteTopic(zkUtils(), kafkaTopic);
    }
    zkUtils.close();
    super.tearDown();
  }

  private void publishKafkaEvents(String topic, int numEvents, String streamProcessorId) {
    KafkaProducer producer = getKafkaProducer();
    for (int eventIndex = 0; eventIndex < numEvents; eventIndex++) {
      try {
        LOGGER.info("Publish kafka event with index : {} for stream processor: {}.", eventIndex, streamProcessorId);
        producer.send(new ProducerRecord(topic, new TestKafkaEvent(streamProcessorId, String.valueOf(eventIndex)).toString().getBytes()));
      } catch (Exception  e) {
        LOGGER.error("Publishing to kafka topic: {} resulted in exception: {}.", new Object[]{topic, e});
        throw new SamzaException(e);
      }
    }
  }

  private ApplicationConfig buildStreamApplicationConfig(String systemName, String inputTopic,
                                                         String processorId, String appName, String appId) {
    Map<String, String> samzaContainerConfig = ImmutableMap.<String, String>builder()
        .put(TaskConfig.INPUT_STREAMS(), inputTopic)
        .put(JobConfig.JOB_DEFAULT_SYSTEM(), systemName)
        .put(TaskConfig.IGNORED_EXCEPTIONS(), "*")
        .put(ZkConfig.ZK_CONNECT, zkConnect())
        .put(JobConfig.SSP_GROUPER_FACTORY(), TEST_SSP_GROUPER_FACTORY)
        .put(TaskConfig.GROUPER_FACTORY(), TEST_TASK_GROUPER_FACTORY)
        .put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, TEST_JOB_COORDINATOR_FACTORY)
        .put(JobConfig.PROCESSOR_ID(), processorId)
        .put(ApplicationConfig.APP_NAME, appName)
        .put(ApplicationConfig.APP_ID, appId)
        .put(String.format("systems.%s.samza.factory", systemName), TEST_SYSTEM_FACTORY)
        .put(JobConfig.JOB_NAME(), TEST_JOB_NAME)
        .build();
    Map<String, String> applicationConfig = Maps.newHashMap(samzaContainerConfig);
    applicationConfig.putAll(StandaloneTestUtils.getKafkaSystemConfigs(systemName, bootstrapServers(), zkConnect(), null, StandaloneTestUtils.SerdeAlias.STRING, true));
    return new ApplicationConfig(new MapConfig(applicationConfig));
  }

  @Test
  public void shouldReElectLeaderWhenLeaderDies() throws InterruptedException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create stream applications.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch3 = new CountDownLatch(1);

    StreamApplication streamApp1 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch);
    StreamApplication streamApp2 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch);
    StreamApplication streamApp3 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch3, null, kafkaEventsConsumedLatch);

    // Run stream applications.
    applicationRunner1.run(streamApp1);
    applicationRunner2.run(streamApp2);
    applicationRunner3.run(streamApp3);

    // Wait until all processors have processed a message.
    processedMessagesLatch1.await();
    processedMessagesLatch2.await();
    processedMessagesLatch3.await();

    // Verifications before killing the leader.
    JobModel jobModel = zkUtils.getJobModel(zkUtils.getJobModelVersion());
    String prevJobModelVersion = zkUtils.getJobModelVersion();
    assertEquals(3, jobModel.getContainers().size());
    assertEquals(Sets.newHashSet("0000000000", "0000000001", "0000000002"), jobModel.getContainers().keySet());
    assertEquals("1", prevJobModelVersion);

    List<String> processorIdsFromZK = zkUtils.getActiveProcessorsIDs(Arrays.asList(PROCESSOR_IDS));

    assertEquals(3, processorIdsFromZK.size());
    assertEquals(PROCESSOR_IDS[0], processorIdsFromZK.get(0));

    // Kill the leader. Since streamApp1 is the first to join the cluster, it's the leader.
    applicationRunner1.kill(streamApp1);
    kafkaEventsConsumedLatch.await();

    // Verifications after killing the leader.
    assertEquals(ApplicationStatus.SuccessfulFinish, applicationRunner1.status(streamApp1));
    processorIdsFromZK = zkUtils.getActiveProcessorsIDs(ImmutableList.of(PROCESSOR_IDS[1], PROCESSOR_IDS[2]));
    assertEquals(2, processorIdsFromZK.size());
    assertEquals(PROCESSOR_IDS[1], processorIdsFromZK.get(0));
    jobModel = zkUtils.getJobModel(zkUtils.getJobModelVersion());
    assertEquals(Sets.newHashSet("0000000001", "0000000002"), jobModel.getContainers().keySet());
    String currentJobModelVersion = zkUtils.getJobModelVersion();
    assertEquals(2, jobModel.getContainers().size());
    assertEquals("2", currentJobModelVersion);
  }

  @Test
  public void shouldFailWhenNewProcessorJoinsWithSameIdAsExistingProcessor() throws InterruptedException {
    // Set up kafka topics.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    // Create StreamApplications.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    StreamApplication streamApp1 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, null, kafkaEventsConsumedLatch);
    StreamApplication streamApp2 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch);

    // Run stream applications.
    applicationRunner1.run(streamApp1);
    applicationRunner2.run(streamApp2);

    // Wait for message processing to start in both the processors.
    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    LocalApplicationRunner applicationRunner3 = new LocalApplicationRunner(new MapConfig(applicationConfig2));

    // Create a stream app with same processor id as SP2 and run it. It should fail.
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[2]);
    kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    StreamApplication streamApp3 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, null, null, kafkaEventsConsumedLatch);
    // Fail when the duplicate processor joins.
    expectedException.expect(Exception.class);
    applicationRunner3.run(streamApp3);
  }

  // Depends upon SAMZA-1302
  // @Test(expected = Exception.class)
  public void shouldKillStreamAppWhenZooKeeperDiesBeforeLeaderReElection() throws InterruptedException {
    // Create StreamApplications.
    StreamApplication streamApp1 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, null, null, null);
    StreamApplication streamApp2 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, null, null, null);

    // Run stream applications.
    applicationRunner1.run(streamApp1);
    applicationRunner2.run(streamApp2);

    applicationRunner1.kill(streamApp1);
    applicationRunner1.waitForFinish();
    assertEquals(ApplicationStatus.SuccessfulFinish, applicationRunner1.status(streamApp2));

    // Kill zookeeper server and expect job model regeneration and ZK fencing will fail with exception.
    zookeeper().shutdown();

    applicationRunner1 = new LocalApplicationRunner(applicationConfig1);
    applicationRunner1.run(streamApp1);
    // This line should throw exception since Zookeeper is unavailable.
    applicationRunner1.waitForFinish();
  }

  // Depends upon SAMZA-1302
  // @Test
  public void testRollingUpgrade() throws Exception {
    publishKafkaEvents(inputKafkaTopic, NUM_KAFKA_EVENTS, PROCESSOR_IDS[0]);

    List<TestKafkaEvent> messagesProcessed = new ArrayList<>();
    StreamApplicationCallback streamApplicationCallback = messagesProcessed::add;

    // Create StreamApplication from configuration.
    CountDownLatch kafkaEventsConsumedLatch = new CountDownLatch(NUM_KAFKA_EVENTS);
    CountDownLatch processedMessagesLatch1 = new CountDownLatch(1);
    CountDownLatch processedMessagesLatch2 = new CountDownLatch(1);

    StreamApplication streamApp1 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch1, streamApplicationCallback, kafkaEventsConsumedLatch);
    StreamApplication streamApp2 = new TestStreamApplication(inputKafkaTopic, outputKafkaTopic, processedMessagesLatch2, null, kafkaEventsConsumedLatch);

    // Run stream application.
    applicationRunner1.run(streamApp1);
    applicationRunner2.run(streamApp2);

    processedMessagesLatch1.await();
    processedMessagesLatch2.await();

    // Read job model before rolling upgrade.
    String jobModelVersion = zkUtils.getJobModelVersion();
    JobModel jobModel = zkUtils.getJobModel(jobModelVersion);

    applicationRunner1.kill(streamApp1);
    applicationRunner1.waitForFinish();

    int lastProcessedMessageId = -1;
    for (TestKafkaEvent message : messagesProcessed) {
      lastProcessedMessageId = Math.max(lastProcessedMessageId, Integer.parseInt(message.getEventId()));
    }

    LocalApplicationRunner applicationRunner4 = new LocalApplicationRunner(applicationConfig1);
    applicationRunner4.run(streamApp1);
    applicationRunner4.waitForFinish();

    // Kill both the stream applications.
    applicationRunner4.kill(streamApp1);
    applicationRunner4.waitForFinish();
    applicationRunner2.kill(streamApp2);
    applicationRunner2.waitForFinish();

    // Read new job model after rolling upgrade.
    String newJobModelVersion = zkUtils.getJobModelVersion();
    JobModel newJobModel = zkUtils.getJobModel(newJobModelVersion);

    // This should be continuation of last processed message.
    int nextSeenMessageId = Integer.parseInt(messagesProcessed.get(0).getEventId());
    assertTrue(lastProcessedMessageId < nextSeenMessageId);

    // Assertions on job model read from zookeeper.
    assertFalse(newJobModelVersion.equals(jobModelVersion));
    assertEquals(jobModel, newJobModel);
    assertEquals(Sets.newHashSet("0000000001", "0000000002"), jobModel.getContainers().keySet());
    String currentJobModelVersion = zkUtils.getJobModelVersion();
    List<String> processorIdsFromZK = zkUtils.getActiveProcessorsIDs(Arrays.asList(PROCESSOR_IDS));
    assertEquals(3, processorIdsFromZK.size());
    assertEquals(ImmutableList.of(PROCESSOR_IDS[1], PROCESSOR_IDS[2]), processorIdsFromZK);
    assertEquals(2, jobModel.getContainers().size());
    assertEquals("2", currentJobModelVersion);
  }

  public interface StreamApplicationCallback {
    void onMessageReceived(TestKafkaEvent message);
  }

  private static class TestKafkaEvent implements Serializable {

    // Actual content of the event.
    private String eventData;

    // Contains Integer value, which is greater than previous message id.
    private String eventId;

    TestKafkaEvent(String eventId, String eventData) {
      this.eventData = eventData;
      this.eventId = eventId;
    }

    String getEventId() {
      return eventId;
    }

    String getEventData() {
      return eventData;
    }

    @Override
    public String toString() {
      return eventId + "|" + eventData;
    }

    static TestKafkaEvent fromString(String message) {
      String[] messageComponents = message.split("|");
      return new TestKafkaEvent(messageComponents[0], messageComponents[1]);
    }
  }

  /**
   * Publishes all input events to output topic(has no processing logic)
   * and triggers {@link StreamApplicationCallback} with each received event.
   **/
  private static class TestStreamApplication implements StreamApplication {

    private final String inputTopic;
    private final String outputTopic;
    private final CountDownLatch processedMessagesLatch;
    private final StreamApplicationCallback streamApplicationCallback;
    private final CountDownLatch kafkaEventsConsumedLatch;

    TestStreamApplication(String inputTopic, String outputTopic,
                          CountDownLatch processedMessagesLatch,
                          StreamApplicationCallback streamApplicationCallback, CountDownLatch kafkaEventsConsumedLatch) {
      this.inputTopic = inputTopic;
      this.outputTopic = outputTopic;
      this.processedMessagesLatch = processedMessagesLatch;
      this.streamApplicationCallback = streamApplicationCallback;
      this.kafkaEventsConsumedLatch = kafkaEventsConsumedLatch;
    }

    @Override
    public void init(StreamGraph graph, Config config) {
      MessageStream<String> inputStream = graph.getInputStream(inputTopic,  (key, msg) -> {
          TestKafkaEvent incomingMessage = TestKafkaEvent.fromString((String) msg);
          if (streamApplicationCallback != null) {
            streamApplicationCallback.onMessageReceived(incomingMessage);
          }
          if (processedMessagesLatch != null) {
            processedMessagesLatch.countDown();
          }
          if (kafkaEventsConsumedLatch != null) {
            kafkaEventsConsumedLatch.countDown();
          }
          return incomingMessage.toString();
        });
      OutputStream<String, String, String> outputStream = graph.getOutputStream(outputTopic, event -> null, event -> event);
      inputStream.sendTo(outputStream);
    }
  }
}
