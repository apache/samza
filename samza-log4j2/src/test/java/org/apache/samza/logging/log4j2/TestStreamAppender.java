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

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Order;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.logging.LoggingContextHolder;
import org.apache.samza.logging.log4j2.serializers.LoggingEventJsonSerde;
import org.apache.samza.logging.log4j2.serializers.LoggingEventStringSerde;
import org.apache.samza.logging.log4j2.serializers.LoggingEventStringSerdeFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static java.lang.Thread.sleep;
import static org.apache.samza.logging.log4j2.StreamAppender.SET_UP_SYSTEM_TIMEOUT_MILLI_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;


public class TestStreamAppender {
  private static final Logger LOG = (Logger) LogManager.getLogger(TestStreamAppender.class);

  @Mock
  private LoggingContextHolder loggingContextHolder;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(this.loggingContextHolder.getConfig()).thenReturn(baseConfig());
  }

  @After
  public void tearDown() {
    removeAllAppenders();
    MockSystemProducer.listeners.clear();
    MockSystemProducer.messagesReceived.clear();
    MockSystemAdmin.createdStreamSpec = null;
  }

  @Test
  public void testDefaultSerde() {
    System.setProperty("samza.container.name", "samza-container-1");
    StreamAppender streamAppender =
        new StreamAppender("testName", null, null, false, false, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    // trigger system set up by sending a log
    LOG.info("log message");
    assertEquals(LoggingEventJsonSerde.class, streamAppender.getSerde().getClass());
    streamAppender.stop();
  }

  @Test
  public void testNonDefaultSerde() {
    System.setProperty("samza.container.name", "samza-container-1");
    Map<String, String> configMap = new HashMap<>();
    configMap.put("job.name", "log4jTest");
    configMap.put("job.id", "1");
    configMap.put("serializers.registry.log4j-string.class", LoggingEventStringSerdeFactory.class.getCanonicalName());
    configMap.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
    configMap.put("systems.mock.streams.__samza_log4jTest_1_logs.samza.msg.serde", "log4j-string");
    configMap.put("task.log4j.system", "mock");
    when(this.loggingContextHolder.getConfig()).thenReturn(new MapConfig(configMap));

    StreamAppender streamAppender =
        new StreamAppender("testName", null, null, false, false, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    // trigger system set up by sending a log
    LOG.info("log message");
    assertEquals(LoggingEventStringSerde.class, streamAppender.getSerde().getClass());
    streamAppender.stop();
  }

  @Test
  public void testSystemProducerAppenderAppend() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    StreamAppender streamAppender =
        new StreamAppender("testName", null, layout, false, false, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    List<String> messages = Lists.newArrayList("testing1", "testing2");
    logAndVerifyMessages(messages);
    streamAppender.stop();
  }

  @Test
  public void testSystemProducerAppenderInContainerWithAsyncLogger() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");
    // Enabling async logger on log4j2 programmatically
    ConfigurationFactory.setConfigurationFactory(new AsyncLoggerConfigurationFactory());

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    StreamAppender streamAppender =
        new StreamAppender("testName", null, layout, false, true, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    List<String> messages = Lists.newArrayList("testing1", "testing2");
    logAndVerifyMessages(messages);
    streamAppender.stop();
  }

  @Plugin(name = "AsyncLoggerConfigurationFactory", category = ConfigurationFactory.CATEGORY)
  @Order(50)
  public static class AsyncLoggerConfigurationFactory extends ConfigurationFactory {

    private static Configuration createConfiguration(final String name, ConfigurationBuilder<BuiltConfiguration> builder) {
      builder.setConfigurationName(name);
      RootLoggerComponentBuilder rootLoggerBuilder = builder.newAsyncRootLogger(Level.INFO);
      builder.add(rootLoggerBuilder);
      return builder.build();
    }

    @Override
    public Configuration getConfiguration(final LoggerContext loggerContext, final ConfigurationSource source) {
      return getConfiguration(loggerContext, source.toString(), null);
    }

    @Override
    public Configuration getConfiguration(final LoggerContext loggerContext, final String name, final URI configLocation) {
      ConfigurationBuilder<BuiltConfiguration> builder = newConfigurationBuilder();
      return createConfiguration(name, builder);
    }

    @Override
    protected String[] getSupportedTypes() {
      return new String[]{"*"};
    }
  }

  @Test
  public void testSystemProducerAppenderNotInitialized() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-job-coordinator");

    when(this.loggingContextHolder.getConfig()).thenReturn(null);
    // add a counter to make sure that the initial message doesn't get produced
    AtomicInteger numMessagesProduced = new AtomicInteger(0);
    MockSystemProducer.listeners.add((source, envelope) -> numMessagesProduced.incrementAndGet());

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    StreamAppender streamAppender =
        new StreamAppender("testName", null, layout, false, false, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);

    LOG.info("no-received"); // System isn't initialized yet, so this message should be dropped

    // make config available so messages now get sent to the stream
    when(this.loggingContextHolder.getConfig()).thenReturn(baseConfig());

    List<String> messages = Lists.newArrayList("testing3", "testing4");
    logAndVerifyMessages(messages);
    streamAppender.stop();
    assertEquals(messages.size(), numMessagesProduced.get());
  }

  @Test
  public void testNoStreamCreationUponSetupByDefault() {
    System.setProperty("samza.container.name", "samza-container-1");

    StreamAppender streamAppender =
        new StreamAppender("testName", null, null, false, false, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    // trigger system set up by sending a log
    LOG.info("log message");
    streamAppender.stop();

    Assert.assertNull(MockSystemAdmin.createdStreamSpec);
  }

  @Test
  public void testStreamCreationDefaultStreamName() {
    System.setProperty("samza.container.name", "samza-container-1");

    MapConfig mapConfig = new MapConfig(ImmutableMap.of(
        "task.log4j.create.stream.enabled", "true", // Enable explicit stream creation
        "job.name", "log4jTest",
        "job.id", "1",
        "systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName(),
        "task.log4j.system", "mock"));
    when(this.loggingContextHolder.getConfig()).thenReturn(mapConfig);

    StreamAppender streamAppender =
        new StreamAppender("testName", null, null, false, false, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    // trigger system set up by sending a log
    LOG.info("log message");
    streamAppender.stop();

    Assert.assertEquals("__samza_log4jTest_1_logs", MockSystemAdmin.createdStreamSpec.getPhysicalName());
    // job.container.count defaults to 1
    Assert.assertEquals(1, MockSystemAdmin.createdStreamSpec.getPartitionCount());
  }

  @Test
  public void testStreamCreationCustomStreamName() {
    System.setProperty("samza.container.name", "samza-container-1");

    MapConfig mapConfig = new MapConfig(ImmutableMap.of(
        "task.log4j.create.stream.enabled", "true", // Enable explicit stream creation
        "job.name", "log4jTest",
        "job.id", "1",
        "systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName(),
        "task.log4j.system", "mock"));
    when(this.loggingContextHolder.getConfig()).thenReturn(mapConfig);

    StreamAppender streamAppender =
        new StreamAppender("testName", null, null, false, false, "test-stream-name", this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    // trigger system set up by sending a log
    LOG.info("log message");
    streamAppender.stop();

    Assert.assertEquals("test-stream-name", MockSystemAdmin.createdStreamSpec.getPhysicalName());
    // job.container.count defaults to 1
    Assert.assertEquals(1, MockSystemAdmin.createdStreamSpec.getPartitionCount());

  }

  @Test
  public void testStreamCreationUpSetupWithJobContainerCountConfigured() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    MapConfig mapConfig = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put("task.log4j.create.stream.enabled", "true") // Enable explicit stream creation
        .put("job.name", "log4jTest")
        .put("job.id", "1")
        .put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName())
        .put("task.log4j.system", "mock")
        .put("job.container.count", "4")
        .build());
    when(this.loggingContextHolder.getConfig()).thenReturn(mapConfig);

    StreamAppender streamAppender =
        new StreamAppender("testName", null, null, false, false, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    // trigger system set up by sending a log
    LOG.info("log message");
    streamAppender.stop();

    Assert.assertEquals("__samza_log4jTest_1_logs", MockSystemAdmin.createdStreamSpec.getPhysicalName());
    Assert.assertEquals(4, MockSystemAdmin.createdStreamSpec.getPartitionCount());
  }

  @Test
  public void testExceptionsDoNotKillTransferThread() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    StreamAppender streamAppender =
        new StreamAppender("testName", null, null, false, false, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);

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
    streamAppender.stop();
  }

  @Test
  public void testQueueTimeout() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    StreamAppender streamAppender =
        new StreamAppender("testName", null, null, false, false, null, this.loggingContextHolder);
    streamAppender.queueTimeoutS = 1;
    startAndAttachAppender(streamAppender);

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

    assertEquals(messages.size() - expectedMessagesSent, streamAppender.metrics.logMessagesDropped.getCount());

    // Allow all the rest of the messages to send.
    waitForTimeout.countDown();

    // Wait for messages
    assertTrue("Thread did not send all messages. Count: " + allMessagesSent.getCount(),
        allMessagesSent.await(60, TimeUnit.SECONDS));
    assertEquals(expectedMessagesSent, MockSystemProducer.messagesReceived.size());
    streamAppender.stop();
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

  @Test
  public void testLogConcurrently() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    StreamAppender streamAppender =
        new StreamAppender("testName", null, layout, false, true, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    List<String> messages = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      messages.add("testing" + i);
    }
    logConcurrentlyAndVerifyMessages(messages);
    streamAppender.stop();
  }

  @Test
  public void testLogRecursively() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");

    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    StreamAppender streamAppender =
        new StreamAppender("testName", null, layout, false, true, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    List<String> messages = Lists.newArrayList("testing1", "testing2");
    logRecursivelyAndVerifyMessages(messages);
    streamAppender.stop();
  }

  @Test
  public void testSetupStreamTimeout() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-1");
    MapConfig mapConfig = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put("task.log4j.create.stream.enabled", "true") // Enable explicit stream creation
        .put("job.name", "log4jTest")
        .put("job.id", "1")
        .put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName())
        .put("task.log4j.system", "mock")
        .put("job.container.count", "4")
        .build());
    when(this.loggingContextHolder.getConfig()).thenReturn(mapConfig);
    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    StreamAppender streamAppender =
        new StreamAppender("testName", null, layout, false, true, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    setupSystemTimeoutAndVerifyMessages(SET_UP_SYSTEM_TIMEOUT_MILLI_SECONDS * 2);
    streamAppender.stop();
  }

  @Test
  public void testSetupStreamNoTimeout() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-2");
    MapConfig mapConfig = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put("task.log4j.create.stream.enabled", "true") // Enable explicit stream creation
        .put("job.name", "log4jTest")
        .put("job.id", "1")
        .put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName())
        .put("task.log4j.system", "mock")
        .put("job.container.count", "4")
        .build());
    when(this.loggingContextHolder.getConfig()).thenReturn(mapConfig);
    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    StreamAppender streamAppender =
        new StreamAppender("testName", null, layout, false, true, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    setupSystemTimeoutAndVerifyMessages(SET_UP_SYSTEM_TIMEOUT_MILLI_SECONDS / 2);
    streamAppender.stop();
  }

  @Test
  public void testSetupStreamException() throws InterruptedException {
    System.setProperty("samza.container.name", "samza-container-2");
    MapConfig mapConfig = new MapConfig(new ImmutableMap.Builder<String, String>()
        .put("task.log4j.create.stream.enabled", "true") // Enable explicit stream creation
        .put("job.name", "log4jTest")
        .put("job.id", "1")
        .put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName())
        .put("task.log4j.system", "mock")
        .put("job.container.count", "4")
        .build());
    when(this.loggingContextHolder.getConfig()).thenReturn(mapConfig);
    PatternLayout layout = PatternLayout.newBuilder().withPattern("%m").build();
    StreamAppender streamAppender =
        new StreamAppender("testName", null, layout, false, true, null, this.loggingContextHolder);
    startAndAttachAppender(streamAppender);
    setupSystemExceptionAndVerifyMessages();
    streamAppender.stop();
  }

  private void logConcurrentlyAndVerifyMessages(List<String> messages) throws InterruptedException {
    // Set up latch
    final CountDownLatch allMessagesSent = new CountDownLatch(messages.size());
    MockSystemProducer.listeners.add((source, envelope) -> allMessagesSent.countDown());
    ExecutorService service = Executors.newFixedThreadPool(10);

    // Log the messages concurrently
    for (String message : messages) {
      service.submit(() -> {
        LOG.info(message);
      });
    }
    // Wait for messages
    assertTrue("Timeout while waiting for StreamAppender to send all messages. Count: " + allMessagesSent.getCount(),
        allMessagesSent.await(60, TimeUnit.SECONDS));

    // MockSystemProducer.messagesReceived is not thread safe, verify allMessagesSent CountDownLatch instead
    assertEquals(0, allMessagesSent.getCount());
    service.shutdown();
  }

  private void logRecursivelyAndVerifyMessages(List<String> messages) throws InterruptedException {
    // Set up latch
    final CountDownLatch allMessagesSent = new CountDownLatch(messages.size());
    MockSystemProducer.listeners.add((source, envelope) -> {
      LOG.info("system producer invoked");
      allMessagesSent.countDown();
    });
    // Log the messages
    messages.forEach(LOG::info);
    // Wait for messages
    assertTrue("Timeout while waiting for StreamAppender to send all messages. Count: " + allMessagesSent.getCount(),
        allMessagesSent.await(60, TimeUnit.SECONDS));

    // Verify
    assertEquals(messages.size(), MockSystemProducer.messagesReceived.size());
  }

  private void setupSystemTimeoutAndVerifyMessages(long setUpSystemTime) throws InterruptedException {
    MockSystemProducer.listeners.clear();
    MockSystemProducer.messagesReceived.clear();
    MockSystemAdmin.listeners.clear();
    List<String> messages = Lists.newArrayList("testing1", "testing2");
    // Set up latch
    final CountDownLatch allMessagesSent = new CountDownLatch(messages.size());
    MockSystemProducer.listeners.add((source, envelope) -> allMessagesSent.countDown());
    MockSystemAdmin.listeners.add(streamSpec -> {
      try {
        // This log should not be sent to system producer as it is logged recursively
        LOG.info("setting up stream");
        // mock setUpSystem time during createStream() call
        sleep(setUpSystemTime);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
    logMessagesConcurrentlyWithRandomOrder(messages);
    // Wait for messages
    allMessagesSent.await(setUpSystemTime * 4, TimeUnit.MILLISECONDS);
    // If the setUpSystem time out, verify only one message sent. otherwise, verify two messages sent
    if (setUpSystemTime >= SET_UP_SYSTEM_TIMEOUT_MILLI_SECONDS) {
      assertEquals(messages.size() - 1, MockSystemProducer.messagesReceived.size());
    } else {
      assertEquals(messages.size(), MockSystemProducer.messagesReceived.size());
    }
  }

  private void setupSystemExceptionAndVerifyMessages() throws InterruptedException {
    MockSystemProducer.listeners.clear();
    MockSystemProducer.messagesReceived.clear();
    MockSystemAdmin.listeners.clear();
    List<String> messages = Lists.newArrayList("testing1", "testing2");
    // Set up latch
    final CountDownLatch allMessagesSent = new CountDownLatch(messages.size());
    MockSystemProducer.listeners.add((source, envelope) -> allMessagesSent.countDown());
    MockSystemAdmin.listeners.add(streamSpec -> {
      // This log should not be sent to system producer as it is logged recursively
      LOG.info("setting up stream");
      throw new RuntimeException("Exception during setting up stream");
    });
    logMessagesConcurrentlyWithRandomOrder(messages);
    // Wait for messages
    allMessagesSent.await(SET_UP_SYSTEM_TIMEOUT_MILLI_SECONDS * 2, TimeUnit.MILLISECONDS);
    // verify no messages sent
    assertEquals(0, MockSystemProducer.messagesReceived.size());
  }

  private void logMessagesConcurrentlyWithRandomOrder(List<String> messages) {
    List<ExecutorService> executorServices = new ArrayList<>();
    for (int i = 0; i < messages.size(); i++) {
      executorServices.add(Executors.newFixedThreadPool(1));
    }
    for (int i = 0; i < messages.size(); i++) {
      // Log the messages with multiple threads, ensure that each message will be handled by one thread.
      // the threads will sleep a random time so the logging order is random
      // The sleep time should be Incomparable smaller than SET_UP_SYSTEM_TIMEOUT_MILLI_SECONDS to make sure that the threads
      // do try to acquire the lock of setUpSystem concurrently
      String message = messages.get(i);
      executorServices.get(i).submit(() -> {
        try {
          sleep((long) (Math.random() * SET_UP_SYSTEM_TIMEOUT_MILLI_SECONDS / 20));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        LOG.info(message);
      });
    }
  }

  private static Config baseConfig() {
    Map<String, String> map = new HashMap<>();
    map.put("job.name", "log4jTest");
    map.put("systems.mock.samza.factory", MockSystemFactory.class.getCanonicalName());
    map.put("task.log4j.system", "mock");
    return new MapConfig(map);
  }

  private static void startAndAttachAppender(StreamAppender streamAppender) {
    streamAppender.start();
    LOG.addAppender(streamAppender);
    LOG.setLevel(Level.INFO);
  }

  private String asJsonMessageSegment(String message) {
    return String.format("\"message\":\"%s\"", message);
  }

  private void removeAllAppenders() {
    Map<String, Appender> allAppenders = LOG.getAppenders();
    for (String name: allAppenders.keySet()) {
      LOG.removeAppender(allAppenders.get(name));
    }
  }
}
