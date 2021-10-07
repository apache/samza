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

package org.apache.samza.logging.log4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.logging.log4j.serializers.LoggingEventJsonSerde;
import org.apache.samza.logging.log4j.serializers.LoggingEventStringSerde;
import org.apache.samza.logging.log4j.serializers.LoggingEventStringSerdeFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestStreamAppender {
  private static final Logger LOG = Logger.getLogger(TestStreamAppender.class);

  @After
  public void tearDown() {
    LOG.removeAllAppenders();
    MockSystemProducer.listeners.clear();
    MockSystemProducer.messagesReceived.clear();
    MockSystemAdmin.createdStreamSpec = null;
  }

  @Test
  public void testDefaultSerde() {
    System.setProperty("samza.container.name", "samza-container-1");
    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender();
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);
    // trigger system set up by sending a log
    LOG.info("log message");
    assertNotNull(systemProducerAppender.getSerde());
    assertEquals(LoggingEventJsonSerde.class, systemProducerAppender.getSerde().getClass());
  }

  @Test
  public void testNonDefaultSerde() {
    System.setProperty("samza.container.name", "samza-container-1");
    String streamName = StreamAppender.getStreamName("log4jTest", "1");
    Map<String, String> map = new HashMap<>();
    map.put("job.name", "log4jTest");
    map.put("job.id", "1");
    map.put("serializers.registry.log4j-string.class", LoggingEventStringSerdeFactory.class.getCanonicalName());
    map.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
    map.put("systems.mock.streams." + streamName + ".samza.msg.serde", "log4j-string");
    map.put("task.log4j.system", "mock");
    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender(new MapConfig(map));
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);
    // trigger system set up by sending a log
    LOG.info("log message");
    assertNotNull(systemProducerAppender.getSerde());
    assertEquals(LoggingEventStringSerde.class, systemProducerAppender.getSerde().getClass());
  }

  @Test
  public void testSystemProducerAppenderInContainer() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender();
    PatternLayout layout = new PatternLayout();
    layout.setConversionPattern("%m");
    systemProducerAppender.setLayout(layout);
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);

    List<String> messages = Lists.newArrayList("testing1", "testing2");
    logAndVerifyMessages(messages);
  }

  @Test
  public void testSystemProducerAppenderNotInitialized() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-job-coordinator");

    // add a counter to make sure that the initial message doesn't get produced
    AtomicInteger numMessagesProduced = new AtomicInteger(0);
    MockSystemProducer.listeners.add((source, envelope) -> numMessagesProduced.incrementAndGet());

    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender(baseConfig(), false);
    PatternLayout layout = new PatternLayout();
    layout.setConversionPattern("%m");
    systemProducerAppender.setLayout(layout);
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);

    LOG.info("no-received"); // System isn't initialized yet, so this message should be dropped

    // explicitly trigger initialization to test that new messages do get sent to the stream
    systemProducerAppender.setupSystem();
    systemProducerAppender.systemInitialized = true;

    List<String> messages = Lists.newArrayList("testing3", "testing4");
    logAndVerifyMessages(messages);
    assertEquals(messages.size(), numMessagesProduced.get());
  }

  @Test
  public void testNoStreamCreationUponSetupByDefault() {
    System.setProperty("samza.container.name", "samza-container-1");

    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender();
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);
    // trigger system set up by sending a log
    LOG.info("log message");

    Assert.assertNull(MockSystemAdmin.createdStreamSpec);
  }

  @Test
  public void testStreamCreationUpSetupWhenEnabled() {
    System.setProperty("samza.container.name", "samza-container-1");

    MapConfig mapConfig = new MapConfig(ImmutableMap.of(
        "task.log4j.create.stream.enabled", "true", // Enable explicit stream creation
        "job.name", "log4jTest",
        "job.id", "1",
        "systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName(),
        "task.log4j.system", "mock"));

    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender(mapConfig);
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);
    // trigger system set up by sending a log
    LOG.info("log message");

    Assert.assertEquals("__samza_log4jTest_1_logs", MockSystemAdmin.createdStreamSpec.getPhysicalName());
    // job.container.count defaults to 1
    Assert.assertEquals(1, MockSystemAdmin.createdStreamSpec.getPartitionCount());
  }

  @Test
  public void testStreamCreationUpSetupWithJobContainerCountConfigured() {
    System.setProperty("samza.container.name", "samza-container-1");

    MapConfig mapConfig = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put("task.log4j.create.stream.enabled", "true") // Enable explicit stream creation
        .put("job.name", "log4jTest")
        .put("job.id", "1")
        .put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName())
        .put("task.log4j.system", "mock")
        .put("job.container.count", "4")
        .build());

    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender(mapConfig);
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);
    // trigger system set up by sending a log
    LOG.info("log message");

    Assert.assertEquals("__samza_log4jTest_1_logs", MockSystemAdmin.createdStreamSpec.getPhysicalName());
    Assert.assertEquals(4, MockSystemAdmin.createdStreamSpec.getPartitionCount());
  }

  @Test
  public void testStreamCreationUpSetupWithPartitionCountConfigured() {
    System.setProperty("samza.container.name", "samza-container-1");

    MapConfig mapConfig = new MapConfig(ImmutableMap.of(
        "task.log4j.create.stream.enabled", "true", // Enable explicit stream creation
        "job.name", "log4jTest",
        "job.id", "1",
        "systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName(),
        "task.log4j.system", "mock"));

    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender(mapConfig);
    systemProducerAppender.setPartitionCount(8);
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);
    // trigger system set up by sending a log
    LOG.info("log message");

    Assert.assertEquals("__samza_log4jTest_1_logs", MockSystemAdmin.createdStreamSpec.getPhysicalName());
    Assert.assertEquals(8, MockSystemAdmin.createdStreamSpec.getPartitionCount());
  }

  @Test
  public void testExceptionsDoNotKillTransferThread() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender();
    PatternLayout layout = new PatternLayout();
    layout.setConversionPattern("%m");
    systemProducerAppender.setLayout(layout);
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);

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
    messages.forEach(LOG::info);

    // Wait for messages
    assertTrue("Thread did not send all messages. Count: " + allMessagesSent.getCount(),
        allMessagesSent.await(60, TimeUnit.SECONDS));
  }

  @Test
  public void testQueueTimeout() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    MockSystemProducerAppender systemProducerAppender = new MockSystemProducerAppender();
    systemProducerAppender.queueTimeoutS = 1;
    PatternLayout layout = new PatternLayout();
    layout.setConversionPattern("%m");
    systemProducerAppender.setLayout(layout);
    systemProducerAppender.activateOptions();
    LOG.addAppender(systemProducerAppender);

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
    messages.forEach(LOG::info);

    assertEquals(messages.size() - expectedMessagesSent, systemProducerAppender.metrics.logMessagesDropped.getCount());

    // Allow all the rest of the messages to send.
    waitForTimeout.countDown();

    // Wait for messages
    assertTrue("Thread did not send all messages. Count: " + allMessagesSent.getCount(),
        allMessagesSent.await(60, TimeUnit.SECONDS));
    assertEquals(expectedMessagesSent, MockSystemProducer.messagesReceived.size());
  }

  private void logAndVerifyMessages(List<String> messages) throws InterruptedException {
    // Set up latch
    final CountDownLatch allMessagesSent = new CountDownLatch(messages.size());
    MockSystemProducer.listeners.add((source, envelope) -> allMessagesSent.countDown());

    // Log the messages
    messages.forEach(LOG::info);

    // Wait for messages
    assertTrue("Timeout while waiting for StreamAppender to send all messages. Count: " + allMessagesSent.getCount(),
        allMessagesSent.await(60, TimeUnit.SECONDS));

    // Verify
    assertEquals(messages.size(), MockSystemProducer.messagesReceived.size());
    for (int i = 0; i < messages.size(); i++) {
      assertTrue("Message mismatch at index " + i,
          new String((byte[]) MockSystemProducer.messagesReceived.get(i)).contains(asJsonMessageSegment(messages.get(i))));
    }
  }

  private String asJsonMessageSegment(String message) {
    return String.format("\"message\":\"%s\"", message);
  }

  private static Config baseConfig() {
    Map<String, String> map = new HashMap<>();
    map.put("job.name", "log4jTest");
    map.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
    map.put("task.log4j.system", "mock");
    return new MapConfig(map);
  }

  /**
   * Mock class which overrides config-related methods in {@link StreamAppender} for testing.
   */
  private static class MockSystemProducerAppender extends StreamAppender {
    private final Config config;
    private final boolean readyToInitialize;

    public MockSystemProducerAppender() {
      this(baseConfig(), true);
    }

    public MockSystemProducerAppender(Config config) {
      this(config, true);
    }

    public MockSystemProducerAppender(Config config, boolean readyToInitialize) {
      this.config = config;
      this.readyToInitialize = readyToInitialize;
    }

    @Override
    boolean readyToInitialize() {
      return this.readyToInitialize;
    }

    @Override
    protected Config getConfig() {
      return config;
    }
  }
}