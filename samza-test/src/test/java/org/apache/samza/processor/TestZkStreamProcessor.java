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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.samza.zk.TestZkUtils;
import org.junit.Assert;
import org.junit.Test;


/**
 * Happy path tests.
 * Start 1, 2, 5 processors and make sure they all consume all the events.
 */
public class TestZkStreamProcessor extends TestZkStreamProcessorBase {

  @Override
  protected String prefix() {
    return "test_ZK_";
  }

  @Test
  public void testSingleStreamProcessor() {
    testStreamProcessor(new String[]{"1"});
  }

  @Test
  public void testTwoStreamProcessors() {
    testStreamProcessor(new String[]{"2", "3"});
  }

  @Test
  public void testFiveStreamProcessors() {
    testStreamProcessor(new String[]{"4", "5", "6", "7", "8"});
  }

  // main test method for happy path with fixed number of processors
  private void testStreamProcessor(String[] processorIds) {

    // create a latch of the size equals to the number of messages
    TestZkStreamProcessorBase.TestStreamTask.endLatch = new CountDownLatch(messageCount);

    // initialize the processors
    StreamProcessor[] streamProcessors = new StreamProcessor[processorIds.length];
    // we need to know when the processor has started
    CountDownLatch[] startWait = new CountDownLatch[processorIds.length];
    for (int i = 0; i < processorIds.length; i++) {
      startWait[i] = new CountDownLatch(1);
      streamProcessors[i] = createStreamProcessor(processorIds[i], map, startWait[i], null);
    }

    // produce messageCount messages, starting with key 0
    produceMessages(0, inputTopic, messageCount);

    // run the processors in separate threads
    Thread[] threads = new Thread[processorIds.length];
    for (int i = 0; i < processorIds.length; i++) {
      threads[i] = runInThread(streamProcessors[i], TestZkStreamProcessorBase.TestStreamTask.endLatch);
      threads[i].start();
      // wait until the processor reports that it has started
      try {
        startWait[i].await(1000, TimeUnit.MILLISECONDS);
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
        stopProcessor(t);
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
   */ public void testStreamProcessorWithAdd() {

    // set number of events we expect to read by both processes in total:
    // p1 - reads 'messageCount' at first
    // p1 and p2 read all messageCount together, since they start from the beginning.
    // so we expect total 3 x messageCounts
    int totalEventsToGenerate = 3 * messageCount;
    TestStreamTask.endLatch = new CountDownLatch(totalEventsToGenerate);

    // create first processor
    CountDownLatch startWait1 = new CountDownLatch(1);
    CountDownLatch stopWait1 = new CountDownLatch(1);
    StreamProcessor sp = createStreamProcessor("20", map, startWait1, stopWait1);

    // produce first batch of messages starting with 0
    produceMessages(0, inputTopic, messageCount);

    // start the first processor
    Thread t1 = runInThread(sp, TestStreamTask.endLatch);
    t1.start();

    // wait until the processor reports that it has started
    waitForProcessorToStartStop(startWait1);

    // make sure it consumes all the messages from the first batch
    waitUntilMessagesLeftN(totalEventsToGenerate - messageCount);

    // start the second processor
    CountDownLatch startWait2 = new CountDownLatch(1);
    StreamProcessor sp2 = createStreamProcessor("21", map, startWait2, null);
    Thread t2 = runInThread(sp2, TestStreamTask.endLatch);
    t2.start();

    // wait until 2nd processor reports that it has started
    waitForProcessorToStartStop(startWait2);

    // wait until the 1st processor reports that it has stopped
    waitForProcessorToStartStop(stopWait1);

    // let the system to publish and distribute the new job model
    TestZkUtils.sleepMs(600);

    // produce the second batch of the messages, starting with 'messageCount'
    produceMessages(messageCount, inputTopic, messageCount);

    // wait until all the events are consumed
    waitUntilMessagesLeftN(0);

    // shutdown both
    try {
      stopProcessor(t1);
      stopProcessor(t2);
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

    // set number of events we expect to read by both processes in total:
    // p1 and p2 - both read messageCount at first and p1 is shutdown, new batch of events is generated
    // and p2 will read all of them from the beginning (+ 2 x messageCounts, total 3 x)
    int totalEventsToGenerate = 3 * messageCount;
    TestStreamTask.endLatch = new CountDownLatch(totalEventsToGenerate);

    // create first processor
    CountDownLatch waitStart1 = new CountDownLatch(1);
    CountDownLatch waitStop1 = new CountDownLatch(1);
    StreamProcessor sp1 = createStreamProcessor("30", map, waitStart1, waitStop1);

    CountDownLatch containerStopped1 = sp1.jcContainerShutdownLatch;

    // start the first processor
    Thread t1 = runInThread(sp1, TestStreamTask.endLatch);
    t1.start();

    // start the second processor
    CountDownLatch waitStart2 = new CountDownLatch(1);
    CountDownLatch waitStop2 = new CountDownLatch(1);
    StreamProcessor sp2 = createStreamProcessor("31", map, waitStart2, waitStop2);
    Thread t2 = runInThread(sp2, TestStreamTask.endLatch);
    t2.start();

    // wait until the processor reports that it has started
    waitForProcessorToStartStop(waitStart1);

    // wait until the processor reports that it has started
    waitForProcessorToStartStop(waitStart2);

    // produce first batch of messages starting with 0
    produceMessages(0, inputTopic, messageCount);

    // make sure they consume all the messages from the first batch
    waitUntilMessagesLeftN(totalEventsToGenerate - messageCount);

    // stop the first processor
    stopProcessor(t1);

    // wait until it's really down
    waitForProcessorToStartStop(waitStop1);

    // processor1 will stop and start again. We wait for its stop to make sure we can count EXACTLY how many messages it reads.
    waitForProcessorToStartStop(containerStopped1);

    // let the system to publish and distribute the new job model
    TestZkUtils.sleepMs(300);

    // produce the second batch of the messages, starting with 'messageCount'
    produceMessages(messageCount, inputTopic, messageCount);

    // wait until p2 consumes all the message by itself;
    waitUntilMessagesLeftN(0);

    // shutdown p2

    try {
      stopProcessor(t2);
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
