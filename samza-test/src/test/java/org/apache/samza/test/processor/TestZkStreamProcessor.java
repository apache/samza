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

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import kafka.utils.TestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.StreamProcessorLifecycleListener;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.StandaloneIntegrationTestHarness;
import org.apache.samza.test.StandaloneTestUtils;
import org.apache.samza.zk.TestZkUtils;
import org.junit.Assert;
import org.junit.Test;

public class TestZkStreamProcessor extends StandaloneIntegrationTestHarness {
  public static int BAD_MESSAGE_KEY = 1000;
  /**
   * Testing a basic identity stream task - reads data from a topic and writes it to another topic
   * (without any modifications)
   *
   * <p>
   * The standalone version in this test uses KafkaSystemFactory and it uses a SingleContainerGrouperFactory. Hence,
   * no matter how many tasks are present, it will always be run in a single processor instance. This simplifies testing
   */

  @Test
  public void testSingleStreamProcessor() {
    testStreamProcessor(new String[]{"1"});
  }

  @Test
  public void testTwoStreamProcessors() {
    testStreamProcessor(new String[]{"1", "2"});
  }

  @Test
  public void testFiveStreamProcessors() {
    testStreamProcessor(new String[]{"1", "2", "3", "4", "5"});
  }

  // main test method for happy path with fixed number of processors
  private void testStreamProcessor(String[] processorIds) {
    final String testSystem = "test-system";
    final String inputTopic = "numbers";
    final String outputTopic = "output";
    final int messageCount = 40;

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);

    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);

    // create a latch of the size == number of messages
    TestStreamTask.endLatch = new CountDownLatch(messageCount);

    StreamProcessor[] streamProcessors = new StreamProcessor[processorIds.length];
    CountDownLatch[] startCountDownLatches = new CountDownLatch[processorIds.length];
    for (int i = 0; i < processorIds.length; i++) {
      startCountDownLatches[i] = new CountDownLatch(1);
      streamProcessors[i] = createStreamProcessor(processorIds[i], map, startCountDownLatches[i], null);
    }
    produceMessages(0, inputTopic, messageCount);

    Thread[] threads = new Thread[processorIds.length];

    for (int i = 0; i < processorIds.length; i++) {
      threads[i] = runInThread(streamProcessors[i], TestStreamTask.endLatch);
      threads[i].start();
      // wait until the processor reports that it has started
      try {
        startCountDownLatches[i].await(1000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        Assert.fail("got interrupted while waiting for the " + i + "th processor to start.");
      }
    }

    // wait until all the events are consumed
    try {
      TestStreamTask.endLatch.await(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Assert.fail("endLatch.await failed with an interruption:" + e.getLocalizedMessage());
    }

    // collect all the threads
    try {
      for (Thread t : threads) {
        synchronized (t) {
          t.notify(); // to stop the thread
        }
        t.join(1000);
      }
    } catch (InterruptedException e) {
      Assert.fail("Failed to join finished thread:" + e.getLocalizedMessage());
    }

    // we should get each value one time
    Map<Integer, Boolean> expectedValues = new HashMap<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      expectedValues.put(i, false);
    }
    verifyNumMessages(outputTopic, expectedValues, messageCount);
  }

  @Test
  // test with adding another processor
  public void testStreamProcessorWithAdd() {
    final String testSystem = "test-system1";
    final String inputTopic = "numbers";
    final String outputTopic = "output";
    final int messageCount = 40;

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);

    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);

    // set number of events we expect two read by both processes in total:
    // p1 - reads 'messageCount' at first and then both p1 and p2 read all ('messageCount') together, since they
    // start from the beginnig.
    // so we expect total 3 x messageCounts
    int totalEventsToGenerate = 3 * messageCount;
    TestStreamTask.endLatch = new CountDownLatch(totalEventsToGenerate);

    // create first processor
    CountDownLatch startCountDownLatch1 = new CountDownLatch(1);
    StreamProcessor sp = createStreamProcessor("1", map, startCountDownLatch1, null);

    // produce first batch of messages starting with 0
    produceMessages(0, inputTopic, messageCount);

    // start the first processor
    Thread t1 = runInThread(sp, TestStreamTask.endLatch);
    t1.start();

    // wait until the processor reports that it has started
    try {
      startCountDownLatch1.await(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the first processor to start.");
    }

    // make sure it consumes all the messages from the first batch
    int attempts = 5;
    while (attempts > 0) {
      long leftEventsCount = TestStreamTask.endLatch.getCount();
      System.out.println("current count = " + leftEventsCount);
      if (leftEventsCount == totalEventsToGenerate - messageCount) { // read first batch
        System.out.println("read all. current count = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the events in the first batch in 5 attempts", attempts > 0);

    // start the second processor
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    StreamProcessor sp2 = createStreamProcessor("2", map, countDownLatch2, null);
    Thread t2 = runInThread(sp2, TestStreamTask.endLatch);
    t2.start();

    // wait until the processor reports that it has started
    try {
      countDownLatch2.await(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the 2nd processor to start.");
    }

    // wait for at least one full debounce time to let the system to publish and distribute the new job model
    TestZkUtils.sleepMs(3000);

    // produce the second batch of the messages, starting with 'messageCount'
    produceMessages(messageCount, inputTopic, messageCount);

    // wait until all the events are consumed
    // make sure it consumes all the messages from the first batch
    attempts = 5;
    while (attempts > 0) {
      long leftEventsCount = TestStreamTask.endLatch.getCount(); // how much is left to read
      System.out.println("2current count = " + leftEventsCount);
      if (leftEventsCount == 0) { // should read all of them
        System.out.println("2read all. current count = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the leftover events in 5 attempts", attempts > 0);

    try {
      synchronized (t1) {
        t1.notify();
      }
      synchronized (t2) {
        t2.notify();
      }
      t1.join(1000);
      t2.join(1000);
    } catch (InterruptedException e) {
      Assert.fail("Failed to join finished threads:" + e.getLocalizedMessage());
    }

    // processor1 will read 40 events, and then processor1 and processor2 will read 80 events together,
    // but the expected values are the same 0-79 - we should get each value one time.
    // Meanwhile the number of events we gonna get is 80+40=120
    Map<Integer, Boolean> expectedValues = new HashMap<>(2 * messageCount);
    for (int i = 0; i < 2 * messageCount; i++) {
      expectedValues.put(i, false);
    }
    verifyNumMessages(outputTopic, expectedValues, totalEventsToGenerate);
  }

  @Test
  // test with a single processor removed
  public void testStreamProcessorWithRemove() {
    final String testSystem = "test-system2";
    final String inputTopic = "numbers";
    final String outputTopic = "output";
    final int messageCount = 40;

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);

    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);

    // set number of events we expect to read by both processes in total:
    // p1 and p2 - both read messageCount at first and p1 is shutdown, new batch of events is generated
    // and p2 will read all of them from the beginning (+ 2 x messageCounts, total 3 x)
    int totalEventsToGenerate = 3 * messageCount;
    TestStreamTask.endLatch = new CountDownLatch(totalEventsToGenerate);

    // create first processor
    CountDownLatch startCountDownLatch1 = new CountDownLatch(1);
    CountDownLatch stopCountDownLatch1 = new CountDownLatch(1);
    StreamProcessor sp1 = createStreamProcessor("1", map, startCountDownLatch1, stopCountDownLatch1);

    // start the first processor
    Thread t1 = runInThread(sp1, TestStreamTask.endLatch);
    t1.start();

    // start the second processor
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    StreamProcessor sp2 = createStreamProcessor("2", map, countDownLatch2, null);
    Thread t2 = runInThread(sp2, TestStreamTask.endLatch);
    t2.start();

    // wait until the processor reports that it has started
    try {
      startCountDownLatch1.await(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the first processor to start.");
    }

    // wait until the processor reports that it has started
    try {
      countDownLatch2.await(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the 2nd processor to start.");
    }

    // produce first batch of messages starting with 0
    produceMessages(0, inputTopic, messageCount);

    // make sure they consume all the messages from the first batch
    int attempts = 5;
    while (attempts > 0) {
      long leftEventsCount = TestStreamTask.endLatch.getCount();
      System.out.println("current count = " + leftEventsCount);
      if (leftEventsCount == totalEventsToGenerate - messageCount) { // read first batch
        System.out.println("read all. current count = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the events in the first batch in 5 attempts", attempts > 0);

    // stop the first processor
    synchronized (t1) {
      t1.notify(); // this should stop it
    }

    // wait until it's really down
    try {
      stopCountDownLatch1.await(1000, TimeUnit.MILLISECONDS);
      System.out.println("Processor 1 is down");
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the 1st processor to stop.");
    }

    // wait for at least one full debounce time to let the system to publish and distribute the new job model
    TestZkUtils.sleepMs(3000);

    // produce the second batch of the messages, starting with 'messageCount'
    produceMessages(messageCount, inputTopic, messageCount);

    // wait until p2 consumes all the message by itself
    attempts = 5;
    while (attempts > 0) {
      long leftEventsCount = TestStreamTask.endLatch.getCount();
      System.out.println("2current count = " + leftEventsCount);
      if (leftEventsCount == 0) { // should read all of them
        System.out.println("2read all. current count = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the leftover events in 5 attempts", attempts > 0);

    // shutdown p2

    try {
      synchronized (t2) {
        t2.notify();
      }
      t2.join(1000);
    } catch (InterruptedException e) {
      Assert.fail("Failed to join finished thread:" + e.getLocalizedMessage());
    }

    // processor1 and 2 will both read 20 events (total 40), and then processor2 read 80 events by itself,
    // but the expected values are the same 0-79 - we should get each value one time.
    // Meanwhile the number of events we gonna get is 40 + 80
    Map<Integer, Boolean> expectedValues = new HashMap<>(2 * messageCount);
    for (int i = 0; i < 2 * messageCount; i++) {
      expectedValues.put(i, false);
    }
    verifyNumMessages(outputTopic, expectedValues, totalEventsToGenerate);
  }

  @Test(expected = org.apache.samza.SamzaException.class)
  public void testZkUnavailable() {
    final String testSystem = "test-system";
    final String inputTopic = "numbers";
    final String outputTopic = "output";
    final int messageCount = 40;

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);
    map.put(ZkConfig.ZK_CONNECT, "localhost:2222"); // non-existing zk
    map.put(ZkConfig.ZK_CONNECTION_TIMEOUT_MS, "3000");

    CountDownLatch startLatch = new CountDownLatch(1);
    createStreamProcessor("1", map, startLatch, null); // this should fail with timeout exception
  }

  @Test
  // test with a single processor failing
  public void testFailStreamProcessor() {
    final String testSystem = "test-system2";
    final String inputTopic = "numbers";
    final String outputTopic = "output";
    final int messageCount = 40;
    final int numBadMessages = 4;

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);

    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);

    // set number of events we expect to read by both processes in total:
    // p1 will read messageCount messages, then it will die.
    // p2 will read messageCount messages + numBadMessages/2, then a new job model will arrive,
    // and p2 will read messageCount messages again, + numBadMessages
    // total 2 x messageCount + messageCount + numBadMessages + numBadMessages/2
    int totalEventsToGenerate = 3 * messageCount + numBadMessages + numBadMessages/2;
    TestStreamTask.endLatch = new CountDownLatch(totalEventsToGenerate);

    // create first processor
    CountDownLatch startCountDownLatch1 = new CountDownLatch(1);
    CountDownLatch stopCountDownLatch1 = new CountDownLatch(1);
    StreamProcessor sp1 = createStreamProcessor("1", map, startCountDownLatch1, stopCountDownLatch1);

    // start the first processor
    Thread t1 = runInThread(sp1, TestStreamTask.endLatch);
    t1.start();

    // start the second processor
    CountDownLatch countDownLatch2 = new CountDownLatch(1);
    StreamProcessor sp2 = createStreamProcessor("2", map, countDownLatch2, null);
    Thread t2 = runInThread(sp2, TestStreamTask.endLatch);
    t2.start();

    // wait until the processor reports that it has started
    try {
      startCountDownLatch1.await(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the first processor to start.");
    }

    // wait until the processor reports that it has started
    try {
      countDownLatch2.await(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the 2nd processor to start.");
    }

    // produce first batch of messages starting with 0
    produceMessages(0, inputTopic, messageCount);

    // make sure they consume all the messages
    int attempts = 5;
    while (attempts > 0) {
      long leftEventsCount = TestStreamTask.endLatch.getCount();
      System.out.println("left to read = " + leftEventsCount);
      if (leftEventsCount == totalEventsToGenerate - messageCount) { // read first batch
        System.out.println("read all. left to read = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the events in the first batch in 5 attempts", attempts > 0);

    // produce a bad message
    produceMessages(BAD_MESSAGE_KEY, inputTopic, 4);

    // wait for at least one full debounce time to let the system to publish and distribute the new job model
    TestZkUtils.sleepMs(3000);

    // produce the second batch of the messages, starting with 'messageCount'
    produceMessages(messageCount, inputTopic, messageCount);

    // wait until p2 consumes all the message by itself
    attempts = 5;
    while (attempts > 0) {
      long leftEventsCount = TestStreamTask.endLatch.getCount();
      System.out.println("2nd left to read = " + leftEventsCount);
      if (leftEventsCount == 0) { // should've read all of them
        System.out.println("2nd read all. left to red  = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the leftover events in 5 attempts", attempts > 0);

    // shutdown p2
    try {
      synchronized (t2) {
        t2.notify();
      }
      t2.join(1000);
    } catch (InterruptedException e) {
      Assert.fail("Failed to join finished thread:" + e.getLocalizedMessage());
    }

    // processor1 and 2 will both read 20 events (total 40), and then processor2 read 80 events by itself,
    // but the expected values are the same 0-79 - we should get each value one time.
    // Meanwhile the number of events we gonna get is 40 + 80
    Map<Integer, Boolean> expectedValues = new HashMap<>(2 * messageCount);
    for (int i = 0; i < 2 * messageCount; i++) {
      expectedValues.put(i, false);
    }

    for (int i = BAD_MESSAGE_KEY; i < numBadMessages + BAD_MESSAGE_KEY; i++) {
      expectedValues.put(i, false);
    }
    verifyNumMessages(outputTopic, expectedValues, totalEventsToGenerate);
  }


  //////////////////////////////////////////////////////////////////////////////////////////////////
  // auxiliary methods
  private StreamProcessor createStreamProcessor(final String pId, Map<String, String> map,
      final CountDownLatch startLatchCountDown, final CountDownLatch stopLatchCountDown) {
    map.put(ApplicationConfig.PROCESSOR_ID, pId);

    StreamProcessor processor = new StreamProcessor(new MapConfig(map), new HashMap<>(), TestStreamTask::new,
        new StreamProcessorLifecycleListener() {

          @Override
          public void onStart() {
            if (startLatchCountDown != null) {
              startLatchCountDown.countDown();
            }
          }

          @Override
          public void onShutdown() {
            if (stopLatchCountDown != null) {
              stopLatchCountDown.countDown();
            }
            System.out.println("ON STOP. PID = " + pId + " in thread " + Thread.currentThread());

          }

          @Override
          public void onFailure(Throwable t) {

          }
        });

    return processor;
  }

  private void createTopics(String inputTopic, String outputTopic) {
    TestUtils.createTopic(zkUtils(), inputTopic, 5, 1, servers(), new Properties());
    TestUtils.createTopic(zkUtils(), outputTopic, 5, 1, servers(), new Properties());
  }

  private Map<String, String> createConfigs(String testSystem, String inputTopic, String outputTopic,
      int messageCount) {
    Map<String, String> configs = new HashMap<>();
    configs.putAll(StandaloneTestUtils
            .getStandaloneConfigs("test-job", "org.apache.samza.test.processor.TestZkStreamProcessor.TestStreamTask"));
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

    return configs;
  }

  /**
   * Produces the provided number of messages to the topic.
   */
  private void produceMessages(final int start, String topic, int numMessages) {
    KafkaProducer producer = getKafkaProducer();
    for (int i = start; i < numMessages + start; i++) {
      try {
        System.out.println("producing " + i);
        producer.send(new ProducerRecord(topic, String.valueOf(i).getBytes())).get();
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Runs the provided stream processor by starting it, waiting on the provided latch with a timeout,
   * and then stopping it.
   */
  private Thread runInThread(final StreamProcessor processor, CountDownLatch latch) {
    Thread t = new Thread() {

      @Override
      public void run() {
        processor.start();
        try {
          // just wait
          synchronized (this) {
            this.wait(100000);
          }
          System.out.println("notified. Abandon the wait.");
        } catch (InterruptedException e) {
          System.out.println("wait interrupted" + e);
        }
        System.out.println("Stopping the processor");
        processor.stop();
      }
    };
    return t;
  }

  /**
   * Consumes data from the topic until there are no new messages for a while
   * and asserts that the number of consumed messages is as expected.
   */
  private void verifyNumMessages(String topic, final Map<Integer, Boolean> expectedValues, int expectedNumMessages) {
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
          //Assert.assertEquals(new String((byte[]) record.value()), String.valueOf(count));
          String val = new String((byte[]) record.value());
          System.out.println("Got value " + val);
          map.put(Integer.valueOf(val), true);
          count++;
        }
      } else {
        emptyPollCount++;
        System.out.println("empty polls " + emptyPollCount);
      }
    }
    // filter out numbers we did not get
    long numFalse = map.values().stream().filter(v -> !v).count();
    Assert.assertEquals("didn't get this number of events ", 0, numFalse);
    Assert.assertEquals(expectedNumMessages, count);
  }

  // StreamTaskClass
  public static class TestStreamTask implements StreamTask, InitableTask {
    // static field since there's no other way to share state b/w a task instance and
    // stream processor when constructed from "task.class".
    static CountDownLatch endLatch;
    private int processedMessageCount = 0;
    private String processorId;
    private String outputTopic;
    private String outputSystem;

    @Override
    public void init(Config config, TaskContext taskContext)
        throws Exception {
      this.processorId = config.get(ApplicationConfig.PROCESSOR_ID);
      this.outputTopic = config.get("app.outputTopic", "output");
      this.outputSystem = config.get("app.outputSystem", "test-system");
    }

    @Override
    public void process(IncomingMessageEnvelope incomingMessageEnvelope, MessageCollector messageCollector,
        TaskCoordinator taskCoordinator)
        throws Exception {

      Object message = incomingMessageEnvelope.getMessage();

      messageCollector.send(new OutgoingMessageEnvelope(new SystemStream(outputSystem, outputTopic), message));
      processedMessageCount++;

      // inject a failure
      if(Integer.valueOf((String)message) >= BAD_MESSAGE_KEY) {
        if(processorId.equals("1")) {
          System.out.println("================================ FAILING for msg=" + message);
          throw new Exception("Processing in the processor " + processorId + " failed ");
        }
      }

      System.out.println(
          "Stream processor " + processorId + ";offset=" + incomingMessageEnvelope.getOffset() + ";received " + message
              + "; ssp=" + incomingMessageEnvelope.getSystemStreamPartition());
      synchronized (endLatch) {
        endLatch.countDown();
      }
    }
  }
}
