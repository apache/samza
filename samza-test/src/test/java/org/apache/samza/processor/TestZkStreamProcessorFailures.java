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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.ZkConfig;
import org.apache.samza.zk.TestZkUtils;
import org.junit.Assert;


/**
 * Failure tests:
 * ZK unavailable.
 * One processor fails in process.
 */
public class TestZkStreamProcessorFailures extends TestZkStreamProcessorBase {

  private final static int BAD_MESSAGE_KEY = 1000;

  @Override
  protected String prefix() {
    return "test_ZK_failure_";
  }

//  @Before
  public void setUp() {
    super.setUp();
  }

  //@Test(expected = org.apache.samza.SamzaException.class)
  public void testZkUnavailable() {
    map.put(ZkConfig.ZK_CONNECT, "localhost:2222"); // non-existing zk
    map.put(ZkConfig.ZK_CONNECTION_TIMEOUT_MS, "3000"); // shorter timeout
    CountDownLatch startLatch = new CountDownLatch(1);
    createStreamProcessor("1", map, startLatch, null); // this should fail with timeout exception
    Assert.fail("should've thrown an exception");
  }

  //@Test
  // Test with a single processor failing.
  // One processor fails (to simulate the failure we inject a special message (id > 1000) which causes the processor to
  // throw an exception.
  public void testFailStreamProcessor() {
    final int numBadMessages = 4; // either of these bad messages will cause p1 to throw and exception
    map.put(JobConfig.JOB_DEBOUNCE_TIME_MS(), "100");
    map.put("processor.id.to.fail", "101");

    // set number of events we expect to read by both processes in total:
    // p1 will read messageCount/2 messages
    // p2 will read messageCount/2 messages
    // numBadMessages "bad" messages will be generated
    // p2 will read 2 of the "bad" messages
    // p1 will fail on the first of the "bad" messages
    // a new job model will be generated
    // and p2 will read all 2 * messageCount messages again, + numBadMessages (all of them this time)
    // total 2 x messageCount / 2 + numBadMessages/2 + 2 * messageCount + numBadMessages
    int totalEventsToBeConsumed = 3 * messageCount;

    TestStreamTask.endLatch = new CountDownLatch(totalEventsToBeConsumed);
    // create first processor
    CountDownLatch waitStart1 = new CountDownLatch(1);
    CountDownLatch waitStop1 = new CountDownLatch(1);
    StreamProcessor sp1 = createStreamProcessor("101", map, waitStart1, waitStop1);
    // start the first processor
    CountDownLatch stopLatch1 = new CountDownLatch(1);
    Thread t1 = runInThread(sp1, stopLatch1);
    t1.start();

    // start the second processor
    CountDownLatch waitStart2 = new CountDownLatch(1);
    CountDownLatch waitStop2 = new CountDownLatch(1);
    StreamProcessor sp2 = createStreamProcessor("102", map, waitStart2, waitStop2);

    CountDownLatch stopLatch2 = new CountDownLatch(1);
    Thread t2 = runInThread(sp2, stopLatch2);
    t2.start();

    // wait until the 1st processor reports that it has started
    waitForProcessorToStartStop(waitStart1);

    // wait until the 2nd processor reports that it has started
    waitForProcessorToStartStop(waitStart2);

    // produce first batch of messages starting with 0
    produceMessages(0, inputTopic, messageCount);

    // make sure they consume all the messages
    waitUntilMessagesLeftN(totalEventsToBeConsumed - messageCount);
    CountDownLatch containerStopped1 = sp1.jcContainerShutdownLatch;
    CountDownLatch containerStopped2 = sp2.jcContainerShutdownLatch;

    // produce the bad messages
    produceMessages(BAD_MESSAGE_KEY, inputTopic, 4);

    waitForProcessorToStartStop(
        containerStopped1); // TODO: after container failure propagates to StreamProcessor change back

    // wait until the 2nd processor reports that it has stopped its container
    waitForProcessorToStartStop(containerStopped2);

    // give some extra time to let the system to publish and distribute the new job model
    TestZkUtils.sleepMs(300);

    // produce the second batch of the messages, starting with 'messageCount'
    produceMessages(messageCount, inputTopic, messageCount);

    // wait until p2 consumes all the message by itself
    waitUntilMessagesLeftN(0);

    // shutdown p2
    try {
      stopProcessor(stopLatch2);
      t2.join(1000);
    } catch (InterruptedException e) {
      Assert.fail("Failed to join finished thread:" + e.getLocalizedMessage());
    }

    // number of unique values we gonna read is from 0 to (2*messageCount - 1)
    Map<Integer, Boolean> expectedValues = new HashMap<>(2 * messageCount);
    for (int i = 0; i < 2 * messageCount; i++) {
      expectedValues.put(i, false);
    }
    for (int i = BAD_MESSAGE_KEY; i < numBadMessages + BAD_MESSAGE_KEY; i++) {
      //expectedValues.put(i, false);
    }

    verifyNumMessages(outputTopic, expectedValues, totalEventsToBeConsumed);
  }
}
