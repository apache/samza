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

package org.apache.samza.logging.log4j2;

import static org.junit.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.samza.config.MapConfig;
import org.apache.samza.logging.log4j2.serializers.LoggingEventJsonSerde;
import org.apache.samza.logging.log4j2.serializers.LoggingEventStringSerde;
import org.apache.samza.logging.log4j2.serializers.LoggingEventStringSerdeFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestStreamAppender {

  static Logger log = (Logger) LogManager.getLogger(TestStreamAppender.class);

  @After
  public void tearDown() {
    removeAllAppenders();
    MockSystemProducer.listeners.clear();
    MockSystemProducer.messagesReceived.clear();
    MockSystemAdmin.createdStreamName = "";
  }

  @Test
  public void testDefaultSerde() {
    System.setProperty("samza.container.name", "samza-container-1");
    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, null, null);
    systemProducerAppender.start();
    assertNotNull(systemProducerAppender.getSerde());
    assertEquals(LoggingEventJsonSerde.class, systemProducerAppender.getSerde().getClass());
  }

  @Test
  public void testNonDefaultSerde() {
    System.setProperty("samza.container.name", "samza-container-1");
    String streamName = StreamAppender.getStreamName("log4jTest", "1");
    Map<String, String> map = new HashMap<String, String>();
    map.put("job.name", "log4jTest");
    map.put("job.id", "1");
    map.put("serializers.registry.log4j-string.class", LoggingEventStringSerdeFactory.class.getCanonicalName());
    map.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
    map.put("systems.mock.streams." + streamName + ".samza.msg.serde", "log4j-string");
    map.put("task.log4j.system", "mock");
    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, new MapConfig(map), null);
    systemProducerAppender.start();
    assertNotNull(systemProducerAppender.getSerde());
    assertEquals(LoggingEventStringSerde.class, systemProducerAppender.getSerde().getClass());
  }

  @Test
  public void testDefaultStreamName() {
    System.setProperty("samza.container.name", "samza-container-1");
    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, null, null);
    systemProducerAppender.start();
    log.addAppender(systemProducerAppender);
    Assert.assertEquals("__samza_log4jTest_1_logs", systemProducerAppender.getStreamName());
  }

  @Test
  public void testCustomStreamName() {
    System.setProperty("samza.container.name", "samza-container-1");
    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, null, "test-stream-name");
    systemProducerAppender.start();
    log.addAppender(systemProducerAppender);
    Assert.assertEquals("test-stream-name", systemProducerAppender.getStreamName());
  }

  @Test
  public void testSystemProducerAppenderInContainer() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, null, null);
    systemProducerAppender.start();

    log.addAppender(systemProducerAppender);
    log.setLevel(Level.INFO);
    List<String> messages = Lists.newArrayList("testing1", "testing2");
    logAndVerifyMessages(messages);
    systemProducerAppender.stop();
  }

  @Test
  public void testSystemProducerAppenderInAM() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-job-coordinator");

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, null, null);
    systemProducerAppender.start();
    log.addAppender(systemProducerAppender);
    log.setLevel(Level.INFO);

    log.info("no-received"); // System isn't initialized yet, so this message should be dropped

    systemProducerAppender.setupSystem();
    MockSystemProducerAppender.systemInitialized = true;

    List<String> messages = Lists.newArrayList("testing3", "testing4");
    logAndVerifyMessages(messages);
    systemProducerAppender.stop();
  }

  @Test
  public void testNoStreamCreationUponSetupByDefault() {
    System.setProperty("samza.container.name", "samza-container-1");

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, null, null);
    systemProducerAppender.start();
    log.addAppender(systemProducerAppender);

    Assert.assertEquals("", MockSystemAdmin.createdStreamName);
  }

  @Test
  public void testStreamCreationUponSetupWhenEnabled() {
    System.setProperty("samza.container.name", "samza-container-1");

    MapConfig mapConfig = new MapConfig(ImmutableMap.of(
        "task.log4j.create.stream.enabled", "true", // Enable explicit stream creation
        "job.name", "log4jTest",
        "job.id", "1",
        "systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName(),
        "task.log4j.system", "mock"));

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, mapConfig, null);
    systemProducerAppender.start();
    log.addAppender(systemProducerAppender);

    Assert.assertEquals("__samza_log4jTest_1_logs", MockSystemAdmin.createdStreamName);
  }

  @Test
  public void testDefaultPartitionCount() {
    System.setProperty("samza.container.name", "samza-container-1");
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, null, false, null, null);
    Assert.assertEquals(1, systemProducerAppender.getPartitionCount()); // job.container.count defaults to 1

    Map<String, String> map = new HashMap<>();
    map.put("job.name", "log4jTest");
    map.put("job.id", "1");
    map.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
    map.put("task.log4j.system", "mock");
    map.put("job.container.count", "4");
    systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, null, false, new MapConfig(map), null);
    Assert.assertEquals(4, systemProducerAppender.getPartitionCount());

    systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, null, false, null, null);
    systemProducerAppender.setPartitionCount(8);
    Assert.assertEquals(8, systemProducerAppender.getPartitionCount());
  }

  @Test
  public void testExceptionsDoNotKillTransferThread() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, null, null);
    systemProducerAppender.start();
    log.addAppender(systemProducerAppender);
    log.setLevel(Level.INFO);

    List<String> messages = Lists.newArrayList("testing5", "testing6", "testing7");

    // Set up latch
    final CountDownLatch allMessagesSent = new CountDownLatch(messages.size());
    MockSystemProducer.listeners.add((source, envelope) -> {
        allMessagesSent.countDown();
        if (allMessagesSent.getCount() == messages.size() - 1) {
          throw new RuntimeException(); // Throw on the first message
        }
      });

    // Log the messages
    messages.forEach((message) -> log.info(message));

    // Wait for messages
    assertTrue("Thread did not send all messages. Count: " + allMessagesSent.getCount(),
        allMessagesSent.await(60, TimeUnit.SECONDS));
    systemProducerAppender.stop();
  }

  @Test
  public void testQueueTimeout() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    MockSystemProducerAppender systemProducerAppender = MockSystemProducerAppender.createAppender("testName", null, layout, false, null, null);
    systemProducerAppender.queueTimeoutS = 1;
    systemProducerAppender.start();
    log.addAppender(systemProducerAppender);
    log.setLevel(Level.INFO);

    int extraMessageCount = 5;
    int expectedMessagesSent = extraMessageCount - 1; // -1 because when the queue is drained there is one additional message that couldn't be added
    List<String> messages = new ArrayList<>(StreamAppender.DEFAULT_QUEUE_SIZE + extraMessageCount);
    for (int i = 0; i < StreamAppender.DEFAULT_QUEUE_SIZE + extraMessageCount; i++) {
      messages.add(String.valueOf(i));
    }

    // Set up latch
    final CountDownLatch allMessagesSent = new CountDownLatch(expectedMessagesSent); // We expect to drop all but the extra messages
    final CountDownLatch waitForTimeout = new CountDownLatch(1);
    MockSystemProducer.listeners.add((source, envelope) -> {
        allMessagesSent.countDown();
        try {
          waitForTimeout.await();
        } catch (InterruptedException e) {
          fail("Test could not run properly because of a thread interrupt.");
        }
      });

    // Log the messages. This is where the timeout will happen!
    messages.forEach((message) -> log.info(message));

    assertEquals(messages.size() - expectedMessagesSent, systemProducerAppender.metrics.logMessagesDropped.getCount());

    // Allow all the rest of the messages to send.
    waitForTimeout.countDown();

    // Wait for messages
    assertTrue("Thread did not send all messages. Count: " + allMessagesSent.getCount(),
        allMessagesSent.await(60, TimeUnit.SECONDS));
    assertEquals(expectedMessagesSent, MockSystemProducer.messagesReceived.size());
    systemProducerAppender.stop();
  }

  private void logAndVerifyMessages(List<String> messages) throws InterruptedException {
    // Set up latch
    final CountDownLatch allMessagesSent = new CountDownLatch(messages.size());
    MockSystemProducer.listeners.add((source, envelope) -> allMessagesSent.countDown());

    // Log the messages
    messages.forEach((message) -> log.info(message));

    // Wait for messages
    assertTrue("Timeout while waiting for StreamAppender to send all messages. Count: " + allMessagesSent.getCount(),
        allMessagesSent.await(60, TimeUnit.SECONDS));

    // Verify
    assertEquals(messages.size(), MockSystemProducer.messagesReceived.size());
    for (int i = 0; i < messages.size(); i++) {
      HashMap<String, String> messageMap = new HashMap<String, String>();
      assertTrue("Message mismatch at index " + i,
          new String((byte[]) MockSystemProducer.messagesReceived.get(i)).contains(asJsonMessageSegment(messages.get(i))));
    }
  }

  private String asJsonMessageSegment(String message) {
    return String.format("\"formatted-message\":\"%s\"", message);
  }

  private void removeAllAppenders() {
    Map<String, Appender> allAppenders = log.getAppenders();
    for (String name: allAppenders.keySet()) {
      log.removeAppender(allAppenders.get(name));
    }
  }
}