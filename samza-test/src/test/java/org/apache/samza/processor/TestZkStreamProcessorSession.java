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
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.zk.ZkJobCoordinator;
import org.junit.Assert;


/**
 * Happy path tests.
 * Start 1, 2, 5 processors and make sure they all consume all the events.
 */
public class TestZkStreamProcessorSession extends TestZkStreamProcessorBase {

  @Override
  protected String prefix() {
    return "test_ZKS_";
  }

  //@Test
  public void testSingleStreamProcessor() {
    testStreamProcessorWithSessionRestart(new String[]{"1"});
  }

  // TODO: SAMZA-1399 fix the flaky test testTwoStreamProcessors and re-enable it
  // @Test
  public void testTwoStreamProcessors() {
    testStreamProcessorWithSessionRestart(new String[]{"2", "3"});
  }

  //@Test
  public void testFiveStreamProcessors() {
    testStreamProcessorWithSessionRestart(new String[]{"4", "5", "6", "7", "8"});
  }

  private void testStreamProcessorWithSessionRestart(String[] processorIds) {

    // set shorter session expiration for the test
    map.put(ZkConfig.ZK_SESSION_TIMEOUT_MS, "500");
    map.put(JobConfig.JOB_DEBOUNCE_TIME_MS(), "3000");

    // create a latch of the size equals to the number of messages
    int totalEventsToGenerate = 3 * messageCount;
    TestZkStreamProcessorBase.TestStreamTask.endLatch = new CountDownLatch(totalEventsToGenerate);

    // initialize the processors
    StreamProcessor[] streamProcessors = new StreamProcessor[processorIds.length];
    ZkJobCoordinator[] jobCoordinators = new ZkJobCoordinator[processorIds.length];
    Thread[] threads = new Thread[processorIds.length];
    CountDownLatch[] threadStopLatches = new CountDownLatch[processorIds.length];
    CountDownLatch[] containerStopLatches = new CountDownLatch[processorIds.length];
    // we need to know when the processor has started
    CountDownLatch[] startWait = new CountDownLatch[processorIds.length];
    //CountDownLatch[] stopWait = new CountDownLatch[processorIds.length];

    for (int i = 0; i < processorIds.length; i++) {
      startWait[i] = new CountDownLatch(1);
      streamProcessors[i] = createStreamProcessor(processorIds[i], map, startWait[i], null);
      jobCoordinators[i] = (ZkJobCoordinator) streamProcessors[i].getCurrentJobCoordinator();
    }

    // run the processors in separate threads
    for (int i = 0; i < processorIds.length; i++) {
      threadStopLatches[i] = new CountDownLatch(1); // is used int stopProcessor
      threads[i] = runInThread(streamProcessors[i], threadStopLatches[i]);
      threads[i].start();
    }

    for (int i = 0; i < processorIds.length; i++) {
      // wait until the processor reports that it has started
      waitForProcessorToStartStop(startWait[i]);
    }

    // produce messageCount messages, starting with key 0
    produceMessages(0, inputTopic, messageCount);

    // make sure it consumes all the messages from the first batch
    waitUntilMessagesLeftN(totalEventsToGenerate - messageCount);

    // Get the container stop latch to be able to check when a container is stopped.
    // New containerShutdownLatch is created after each onNewJobModel,
    // so we need to get the current one, before it changed..
    for (int i = 0; i < processorIds.length; i++) {
      containerStopLatches[i] = streamProcessors[i].containerShutdownLatch;
    }

    // expire zk session of one of the processors
    expireSession(jobCoordinators[0].getZkUtils().getZkClient());

    // wait until all other processors report that they have stopped their containers
    for (int i = 0; i < processorIds.length; i++) {
      waitForProcessorToStartStop(containerStopLatches[i]);
    }

    // read again the first batch
    waitUntilMessagesLeftN(totalEventsToGenerate - 2 * messageCount);

    produceMessages(messageCount, inputTopic, messageCount);

    waitUntilMessagesLeftN(0);

    // collect all the threads
    try {
      for (int i = 0; i < threads.length; i++) {
        stopProcessor(threadStopLatches[i]);
        threads[i].join(1000);
      }
    } catch (InterruptedException e) {
      Assert.fail("Failed to join finished thread:" + e.getLocalizedMessage());
    }

    verifyNumMessages(outputTopic, 2 * messageCount, totalEventsToGenerate);
  }
}

