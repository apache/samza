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

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.zk.TestZkUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Happy path tests.
 * Start 1, 2, 5 processors and make sure they all consume all the events.
 */
public class TestZkStreamProcessor extends TestZkStreamProcessorBase {

  private final static int ATTEMPTS_NUMBER = 5; // to avoid long sleeps, we rather use multiple attempts with shorter sleeps
  
  @Before
  public void setupTest() {

  }

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
    final String testSystem = "test-system" + processorIds.length; // making system unique per test
    final String inputTopic = "numbers" + processorIds.length; // making input topic unique per test
    final String outputTopic = "output" + processorIds.length; // making output topic unique per test
    final int messageCount = 40;

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);

    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);

    // create a latch of the size == number of messages
    TestZkStreamProcessorBase.TestStreamTask.endLatch = new CountDownLatch(messageCount);

    // initialize the the processors
    // we need startLatch to know when the processor has been completely initialized
    StreamProcessor[] streamProcessors = new StreamProcessor[processorIds.length];
    CountDownLatch[] startCountDownLatches = new CountDownLatch[processorIds.length];
    for (int i = 0; i < processorIds.length; i++) {
      startCountDownLatches[i] = new CountDownLatch(1);
      streamProcessors[i] = createStreamProcessor(processorIds[i], map, startCountDownLatches[i], null);
    }

    // produce messageCount messages, starting with key '0'
    produceMessages(0, inputTopic, messageCount);

    // run the processors in a separate threads
    Thread[] threads = new Thread[processorIds.length];
    for (int i = 0; i < processorIds.length; i++) {
      threads[i] = runInThread(streamProcessors[i], TestZkStreamProcessorBase.TestStreamTask.endLatch);
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
      TestZkStreamProcessorBase.TestStreamTask.endLatch.await(10, TimeUnit.SECONDS);
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

    verifyNumMessages(outputTopic, messageCount, messageCount);
  }

  @Test
  /**
   * Similar to the previous tests, but add another processor in the middle
   */
  public void testStreamProcessorWithAdd() {
    final String testSystem = "test-system1";
    final String inputTopic = "numbers_add";
    final String outputTopic = "output_add";
    final int messageCount = 40;

    final Map<String, String> map = createConfigs(testSystem, inputTopic, outputTopic, messageCount);

    // Note: createTopics needs to be called before creating a StreamProcessor. Otherwise it fails with a
    // TopicExistsException since StreamProcessor auto-creates them.
    createTopics(inputTopic, outputTopic);

    // set number of events we expect wo read by both processes in total:
    // p1 - reads 'messageCount' at first
    // p1 and p2 read all messageCount together, since they start from the beginning.
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
    int attempts = ATTEMPTS_NUMBER;
    while (attempts > 0) {
      long leftEventsCount = TestStreamTask.endLatch.getCount();
      System.out.println("messages left to consume = " + leftEventsCount);
      if (leftEventsCount == totalEventsToGenerate - messageCount) { // read first batch
        System.out.println("read first batch. left to consume = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the events in the first batch in " + ATTEMPTS_NUMBER + " attempts", attempts > 0);

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
    attempts = ATTEMPTS_NUMBER;
    while (attempts > 0) {
      long leftEventsCount = TestStreamTask.endLatch.getCount(); // how much is left to read
      System.out.println("2 processors together. left to consume = " + leftEventsCount);
      if (leftEventsCount == 0) { // should read all of them
        System.out.println("2 processors together. read all. left " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the leftover events in " + ATTEMPTS_NUMBER + " attempts", attempts > 0);

    // shutdown both
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

    // p1 will read messageCount events, and then p1 and p2 will read 2xmessageCount events together,
    // but the expected values are the same 0-79, they will appear in the output more then once, but we should mark then only one time.
    // total number of events we gonna get is 80+40=120
    verifyNumMessages(outputTopic, 2 * messageCount, totalEventsToGenerate);
  }

  @Test
  /**
   * same as other happy path messages, but with one processor removed in the middle
   */
  public void testStreamProcessorWithRemove() {
    final String testSystem = "test-system2";
    final String inputTopic = "numbers_rm";
    final String outputTopic = "output_rm";
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
    int attempts = ATTEMPTS_NUMBER;
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
    Assert.assertTrue("Didn't read all the events in the first batch in " + ATTEMPTS_NUMBER + " attempts", attempts > 0);

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

    // wait until p2 consumes all the message by itself;
    attempts = ATTEMPTS_NUMBER;
    while (attempts > 0) {
      long leftEventsCount = TestZkStreamProcessorBase.TestStreamTask.endLatch.getCount();
      System.out.println("2current count = " + leftEventsCount);
      if (leftEventsCount == 0) { // should read all of them
        System.out.println("2read all. current count = " + leftEventsCount);
        break;
      }
      TestZkUtils.sleepMs(1000);
      attempts--;
    }
    Assert.assertTrue("Didn't read all the leftover events in " + ATTEMPTS_NUMBER + " attempts", attempts > 0);

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
    verifyNumMessages(outputTopic, 2 * messageCount, totalEventsToGenerate);
  }
}
