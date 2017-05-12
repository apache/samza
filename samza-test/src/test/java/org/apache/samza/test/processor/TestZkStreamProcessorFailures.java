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
import java.util.stream.Collectors;
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
import org.junit.Before;
import org.junit.Test;


/**
 * Failure tests:
 * ZK unavailable
 * One processor fails
 */
public class TestZkStreamProcessorFailures extends TestZkStreamProcessorBase {

  private final static int ATTEMPTS_NUMBER = 5;
  // to avoid long sleeps, we rather use multiple attempts with shorter sleeps
  private final static int BAD_MESSAGE_KEY = 1000;

  @Before
  public void setupTest() {
  }

  @Test(expected = org.apache.samza.SamzaException.class)
  public void testZkUnavailable() {
    final String testSystem = "test-system-zkUn";
    final String inputTopic = "numbers_zkUn";
    final String outputTopic = "output_zkUn";
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
    final String inputTopic = "numbers_failP";
    final String outputTopic = "output_failP";

    final int messageCount = 40;
    final int numBadMessages = 4; // either of these bad messages will cause p1 to throw and exception

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);
    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);

    // set number of events we expect to read by both processes in total:
    // p1 will read messageCount/2 messages
    // p2 will read messageCount/2 messages
    // numBadMessages bad messages will be generated
    // p2 will read 2 of them
    // p1 will fail on the first of them
    // a new job model will be generated
    // and p2 will read all 2 * messageCount messages again, + numBadMessages (all of them this time)
    // total 2 x messageCount / 2 + numBadMessages/2 + 2 * messageCount + numBadMessages
    int totalEventsToBeConsumed = 3 * messageCount + numBadMessages + numBadMessages / 2;

    TestStreamTask.endLatch = new CountDownLatch(totalEventsToBeConsumed);
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
    // wait until the 2nd processor reports that it has started
    try {
      countDownLatch2.await(1000, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Assert.fail("got interrupted while waiting for the 2nd processor to start.");
    }

    // produce first batch of messages starting with 0
    produceMessages(0, inputTopic, messageCount);

    // make sure they consume all the messages
    int attempts = ATTEMPTS_NUMBER;
    while (attempts > 0) {
      // we wait until messageCount is consumed
      long leftEventsCount = TestStreamTask.endLatch.getCount();
      System.out.println("left to read = " + leftEventsCount);
      if (leftEventsCount == totalEventsToBeConsumed - messageCount) { // read first batch
        System.out.println("read all available. left to read = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the events in the first batch in 5 attempts", attempts > 0);

    // produce the bad messages
    produceMessages(BAD_MESSAGE_KEY, inputTopic, 4);

    // wait for at least one full debounce time to let the system to publish and distribute the new job model
    TestZkUtils.sleepMs(3000);

    // produce the second batch of the messages, starting with 'messageCount'
    produceMessages(messageCount, inputTopic, messageCount);

    // wait until p2 consumes all the message by itself
    attempts = ATTEMPTS_NUMBER;
    while (attempts > 0) {
      long leftEventsCount = TestStreamTask.endLatch.getCount();
      System.out.println("2nd left to read = " + leftEventsCount);
      if (leftEventsCount == 0) { // should've read all of them
        System.out.println("2nd read all, left to read  = " + leftEventsCount);
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

    // number of unique values we gonna read is 0-(2*messageCount - 1) + numBadMessages
    Map<Integer, Boolean> expectedValues = new HashMap<>(2 * messageCount + numBadMessages);
    for (int i = 0; i < 2 * messageCount; i++) {
      expectedValues.put(i, false);
    }
    for (int i = BAD_MESSAGE_KEY; i < numBadMessages + BAD_MESSAGE_KEY; i++) {
      expectedValues.put(i, false);
    }

    verifyNumMessages(outputTopic, expectedValues, totalEventsToBeConsumed);
  }
}
