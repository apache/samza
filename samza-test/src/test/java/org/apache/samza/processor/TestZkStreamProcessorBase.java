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

package org.apache.samza.processor;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import kafka.utils.TestUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfigJava;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobCoordinatorFactory;
import org.apache.samza.runtime.ProcessorLifecycleListener;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.StandaloneIntegrationTestHarness;
import org.apache.samza.test.StandaloneTestUtils;
import org.apache.samza.util.Util;
import org.apache.samza.zk.TestZkUtils;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestZkStreamProcessorBase extends StandaloneIntegrationTestHarness {
  private static final String TASK_SHUTDOWN_MS = "2000";
  private static final String JOB_DEBOUNCE_TIME_MS = "2000";
  private static final String BARRIER_TIMEOUT_MS = "2000";
  private static final String ZK_SESSION_TIMEOUT_MS = "2000";
  private static final String ZK_CONNECTION_TIMEOUT_MS = "2000";

  public final static Logger LOG = LoggerFactory.getLogger(TestZkStreamProcessorBase.class);
  public final static int BAD_MESSAGE_KEY = 1000;
  // to avoid long sleeps, we rather use multiple attempts with shorter sleeps
  protected final static int ATTEMPTS_NUMBER = 5;

  protected AtomicInteger counter = new AtomicInteger(1);
  protected String testSystem;
  protected String inputTopic;
  protected String outputTopic;
  protected int messageCount = 40;

  protected Map<String, String> map;

  protected String prefix() {
    return "";
  }

  @Before
  public void setUp() {
    super.setUp();
    // for each tests - make the common parts unique
    int seqNum = counter.getAndAdd(1);
    testSystem = prefix() + "test-system" + seqNum;
    inputTopic = prefix() + "numbers" + seqNum;
    outputTopic = prefix() + "output" + seqNum;

    map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);

    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);
  }

  // session expiration simulation
  // have to use the reflection to get the session id
  protected void expireSession(ZkClient zkClient) {
    ZkConnection zkConnection = null;
    try {
      Field privateField = ZkClient.class.getDeclaredField("_connection");
      privateField.setAccessible(true);
      zkConnection = (ZkConnection) privateField.get(zkClient);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      Assert.fail(e.toString());
    }

    ZooKeeper zookeeper = zkConnection.getZookeeper();
    long sessionId = zookeeper.getSessionId();

    LOG.info("Closing/expiring session:" + sessionId);
    ZooKeeperServer zkServer = zookeeper().zookeeper();
    zkServer.closeSession(sessionId);
  }

  protected StreamProcessor createStreamProcessor(final String pId, Map<String, String> map, final CountDownLatch waitStart,
      final CountDownLatch waitStop) {
    map.put(ApplicationConfig.PROCESSOR_ID, pId);

    Config config = new MapConfig(map);
    String jobCoordinatorFactoryClassName = new JobCoordinatorConfig(config).getJobCoordinatorFactoryClassName();
    JobCoordinator jobCoordinator = Util.getObj(jobCoordinatorFactoryClassName, JobCoordinatorFactory.class).getJobCoordinator(config);

    ProcessorLifecycleListener listener = new ProcessorLifecycleListener() {
      @Override
      public void beforeStart() {

      }

      @Override
      public void afterStart() {
        if (waitStart != null) {
            waitStart.countDown();
        }
        LOG.info("onStart is called for pid=" + pId);
      }

      @Override
      public void afterStop() {
        // stopped w/o failure
        if (waitStop != null) {
          waitStop.countDown();
        }
        LOG.info("afterStop is called for pid=" + pId + " with successful shutdown");
      }

      @Override
      public void afterFailure(Throwable t) {
        // stopped w/ failure
        LOG.info("afterStop is called for pid=" + pId + " with failure");
      }
    };

    StreamProcessor processor =
        new StreamProcessor(config, new HashMap<>(), (StreamTaskFactory) TestStreamTask::new, listener, jobCoordinator);

    return processor;
  }

  protected void createTopics(String inputTopic, String outputTopic) {
    TestUtils.createTopic(zkUtils(), inputTopic, 5, 1, servers(), new Properties());
    TestUtils.createTopic(zkUtils(), outputTopic, 5, 1, servers(), new Properties());
  }

  protected Map<String, String> createConfigs(String testSystem, String inputTopic, String outputTopic,
      int messageCount) {
    Map<String, String> configs = new HashMap<>();
    configs.putAll(StandaloneTestUtils
        .getStandaloneConfigs("test-job", "org.apache.samza.processor.TestZkStreamProcessor.TestStreamTask"));
    configs.putAll(StandaloneTestUtils
        .getKafkaSystemConfigs(testSystem, bootstrapServers(), zkConnect(), null, StandaloneTestUtils.SerdeAlias.STRING,
            true));
    configs.put("task.inputs", String.format("%s.%s", testSystem, inputTopic));
    configs.put("app.messageCount", String.valueOf(messageCount));
    configs.put("app.outputTopic", outputTopic);
    configs.put("app.outputSystem", testSystem);
    configs.put(ZkConfig.ZK_CONNECT, zkConnect());

    configs.put("job.systemstreampartition.grouper.factory",
        "org.apache.samza.container.grouper.stream.GroupByPartitionFactory");
    configs.put("task.name.grouper.factory", "org.apache.samza.container.grouper.task.GroupByContainerIdsFactory");

    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.zk.ZkJobCoordinatorFactory");
    configs.put(TaskConfigJava.TASK_SHUTDOWN_MS, TASK_SHUTDOWN_MS);
    configs.put(JobConfig.JOB_DEBOUNCE_TIME_MS(), JOB_DEBOUNCE_TIME_MS);
    configs.put(ZkConfig.ZK_CONSENSUS_TIMEOUT_MS, BARRIER_TIMEOUT_MS);
    configs.put(ZkConfig.ZK_SESSION_TIMEOUT_MS, ZK_SESSION_TIMEOUT_MS);
    configs.put(ZkConfig.ZK_CONNECTION_TIMEOUT_MS, ZK_CONNECTION_TIMEOUT_MS);

    return configs;
  }

  /**
   * Produces the provided number of messages to the topic.
   */
  protected void produceMessages(final int start, String topic, int numMessages) {
    KafkaProducer producer = getKafkaProducer();
    for (int i = start; i < numMessages + start; i++) {
      try {
        LOG.info("producing " + i);
        producer.send(new ProducerRecord(topic, i % 2, String.valueOf(i), String.valueOf(i).getBytes())).get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Runs the provided stream processor by starting it, waiting on the provided latch with a timeout,
   * and then stopping it.
   */
  protected Thread runInThread(final StreamProcessor processor, CountDownLatch stopStartLatch) {
    Thread t = new Thread() {

      @Override
      public void run() {
        LOG.info("about to start processor " + processor);
        processor.start();
        LOG.info("started processor " + processor);
        try {
          // just wait
          if (!stopStartLatch.await(1000000, TimeUnit.MILLISECONDS)) {
            LOG.warn("Wait timed out for processor " + processor);
            Assert.fail("Wait timed out for processor " + processor);
          }
          LOG.info("notified. Abandon the wait for processor " + processor);
        } catch (InterruptedException e) {
          LOG.error("wait interrupted" + e);
        }
        LOG.info("Stopping the processor" + processor);
        processor.stop();
        LOG.info("Stopped the processor" + processor);
      }
    };
    return t;
  }

  // for sequential values we can generate them automatically
  protected void verifyNumMessages(String topic, int numberOfSequentialValues, int expectedNumMessages) {
    // we should get each value one time
    // create a map of all expected values to validate
    Map<Integer, Boolean> expectedValues = new HashMap<>(numberOfSequentialValues);
    for (int i = 0; i < numberOfSequentialValues; i++) {
      expectedValues.put(i, false);
    }
    verifyNumMessages(topic, expectedValues, expectedNumMessages);
  }

  /**
   * Consumes data from the topic until there are no new messages for a while
   * and asserts that the number of consumed messages is as expected.
   */
  protected void verifyNumMessages(String topic, final Map<Integer, Boolean> expectedValues, int expectedNumMessages) {
    KafkaConsumer consumer = getKafkaConsumer();
    consumer.subscribe(Collections.singletonList(topic));

    Map<Integer, Boolean> map = new HashMap<>(expectedValues);
    int count = 0;
    int emptyPollCount = 0;

    while (count < expectedNumMessages && emptyPollCount < 5) {
      ConsumerRecords records = consumer.poll(5000);
      if (!records.isEmpty()) {
        Iterator<ConsumerRecord> iterator = records.iterator();
        while (iterator.hasNext()) {
          ConsumerRecord record = iterator.next();
          String val = new String((byte[]) record.value());
          LOG.info("Got value " + val + "; count = " + count + "; out of " + expectedNumMessages);
          Integer valI = Integer.valueOf(val);
          if (valI < BAD_MESSAGE_KEY) {
            map.put(valI, true);
            count++;
          }
        }
      } else {
        emptyPollCount++;
        LOG.warn("empty polls " + emptyPollCount);
      }
    }
    // filter out numbers we did not get
    long numFalse = map.values().stream().filter(v -> !v).count();
    Assert.assertEquals("didn't get this number of events ", 0, numFalse);
    Assert.assertEquals(expectedNumMessages, count);
  }

  protected void waitUntilMessagesLeftN(int untilLeft) {
    int attempts = ATTEMPTS_NUMBER;
    while (attempts > 0) {
      long leftEventsCount = TestZkStreamProcessorBase.TestStreamTask.endLatch.getCount();
      //System.out.println("2current count = " + leftEventsCount);
      if (leftEventsCount == untilLeft) { // that much should be left
        System.out.println("2read all. current count = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(5000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the leftover events in " + ATTEMPTS_NUMBER + " attempts", attempts > 0);
  }

  protected void waitForProcessorToStartStop(CountDownLatch waitObject) {
    LOG.info("Waiting on " + waitObject);
    try {
      if (!waitObject.await(30000, TimeUnit.MILLISECONDS)) {
        Assert.fail("Timed out while waiting for the processor to start/stop.");
      }
    } catch (InterruptedException e) {
      Assert.fail("Got interrupted while waiting for the processor to start/stop.");
    }
  }

  protected void stopProcessor(CountDownLatch stopLatch) {
    stopLatch.countDown();
  }

  // StreamTaskClass
  public static class TestStreamTask implements StreamTask, InitableTask {
    // static field since there's no other way to share state b/w a task instance and
    // stream processor when constructed from "task.class".
    public static CountDownLatch endLatch;
    protected int processedMessageCount = 0;
    protected String processorId;
    protected String outputTopic;
    protected String outputSystem;
    protected String processorIdToFail;

    @Override
    public void init(Config config, TaskContext taskContext)
        throws Exception {
      this.processorId = config.get(ApplicationConfig.PROCESSOR_ID);
      this.outputTopic = config.get("app.outputTopic", "output");
      this.outputSystem = config.get("app.outputSystem", "test-system");
      this.processorIdToFail = config.get("processor.id.to.fail", "1");
    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector,
        TaskCoordinator taskCoordinator)
        throws Exception {

      Object message = incomingMessageEnvelope.getMessage();

      String key = new String((byte[]) incomingMessageEnvelope.getKey());
      Integer val = Integer.valueOf((String) message);

      LOG.info("Stream processor " + processorId + ";key=" + key + ";offset=" + incomingMessageEnvelope.getOffset()
          + "; totalRcvd=" + processedMessageCount + ";val=" + val + "; ssp=" + incomingMessageEnvelope
          .getSystemStreamPartition());

      // inject a failure
      if (val >= BAD_MESSAGE_KEY && processorId.equals(processorIdToFail)) {
        LOG.info("process method failing for msg=" + message);
        throw new Exception("Processing in the processor " + processorId + " failed ");
      }

      messageCollector.send(new OutgoingMessageEnvelope(new SystemStream(outputSystem, outputTopic), message));
      processedMessageCount++;

      synchronized (endLatch) {
        if (Integer.valueOf(key) < BAD_MESSAGE_KEY) {
          endLatch.countDown();
        }
      }
    }
  }
}
