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
package org.apache.samza.test.operator;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableList;
import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemoryOutputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.operator.data.PageView;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class TestAsyncFlatMap {
  private static final String TEST_SYSTEM = "test";
  private static final String PAGE_VIEW_STREAM = "test-async-page-view-stream";
  private static final String NON_GUEST_PAGE_VIEW_STREAM = "test-async-non-guest-page-view-stream";
  private static final String FAIL_PROCESS = "process.fail";
  private static final String FAIL_DOWNSTREAM_OPERATOR = "downstream.operator.fail";
  private static final String LOGIN_PAGE = "login-page";
  private static final String PROCESS_JITTER = "process.jitter";

  private static final List<PageView> PAGE_VIEWS = ImmutableList.of(
      new PageView("1", LOGIN_PAGE, "1"),
      new PageView("2", "home-page", "2"),
      new PageView("3", "profile-page", "0"),
      new PageView("4", LOGIN_PAGE, "0"));

  @Test
  public void testProcessingFutureCompletesSuccessfully() {
    List<PageView> expectedPageViews = PAGE_VIEWS.stream()
        .filter(pageView -> !pageView.getPageId().equals(LOGIN_PAGE) && Long.parseLong(pageView.getUserId()) > 0)
        .collect(Collectors.toList());

    List<PageView> actualPageViews = runTest(new HashMap<>());
    assertEquals("Mismatch between expected vs actual page views", expectedPageViews, actualPageViews);
  }

  @Test
  public void testProcessingFutureCompletesAfterTaskTimeout() {
    Map<String, String> configs = new HashMap<>();
    configs.put(TaskConfig.CALLBACK_TIMEOUT_MS, "100");
    configs.put(PROCESS_JITTER, "2000");

    try {
      runTest(configs);
      fail("App execution should have failed due to a task callback timeout");
    } catch (SamzaException e) {
      /*
       * TestRunner throws SamzaException on failures in general, so check the actual cause. The timeout message is
       * nested within a bunch of other exceptions.
       */
      Throwable rootCause = findRootCause(e);
      assertTrue(String.format("Got exception %s with message %s", rootCause.getClass(), rootCause.getMessage()),
          rootCause instanceof SamzaException);
      // the "{}" is intentional, since the exception message actually includes it (probably a logging bug)
      assertEquals("Callback for task {} Partition 0 timed out after 100 ms.", rootCause.getMessage());
    }
  }

  @Test
  public void testProcessingExceptionIsBubbledUp() {
    Map<String, String> configs = new HashMap<>();
    configs.put(FAIL_PROCESS, "true");

    try {
      runTest(configs);
      fail("App execution should have failed due to a ProcessFailureException");
    } catch (SamzaException e) {
      /*
       * TestRunner throws SamzaException on failures in general, so check the actual cause. The actual exception is
       * nested within a bunch of other exceptions.
       */
      Throwable rootCause = findRootCause(e);
      assertTrue(String.format("Got exception %s with message %s", rootCause.getClass(), rootCause.getMessage()),
          rootCause instanceof ProcessFailureException);
    }
  }

  @Test
  public void testDownstreamOperatorExceptionIsBubbledUp() {
    Map<String, String> configs = new HashMap<>();
    configs.put(FAIL_DOWNSTREAM_OPERATOR, "true");

    try {
      runTest(configs);
      fail("App execution should have failed due to a FilterFailureException");
    } catch (SamzaException e) {
      /*
       * TestRunner throws SamzaException on failures in general, so check the actual cause. The actual exception is
       * nested within a bunch of other exceptions.
       */
      Throwable rootCause = findRootCause(e);
      assertTrue(String.format("Got exception %s with message %s", rootCause.getClass(), rootCause.getMessage()),
          rootCause instanceof FilterFailureException);
    }
  }

  private List<PageView> runTest(Map<String, String> configs) {
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor(TEST_SYSTEM);
    InMemoryInputDescriptor<PageView> pageViewStreamDesc = isd
        .getInputDescriptor(PAGE_VIEW_STREAM, new NoOpSerde<>());
    InMemoryOutputDescriptor<PageView> outputStreamDesc = isd
        .getOutputDescriptor(NON_GUEST_PAGE_VIEW_STREAM, new NoOpSerde<>());

    TestRunner
        .of(new AsyncFlatMapExample())
        .addInputStream(pageViewStreamDesc, PAGE_VIEWS)
        .addOutputStream(outputStreamDesc, 1)
        .addConfig(new MapConfig(configs))
        .run(Duration.ofSeconds(10));

    Map<Integer, List<PageView>> result = TestRunner.consumeStream(outputStreamDesc, Duration.ofMillis(1000));
    return result.values().stream()
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  private static Throwable findRootCause(Throwable e) {
    Throwable currentException = e;
    while (currentException.getCause() != null) {
      currentException = currentException.getCause();
    }
    return currentException;
  }

  static class AsyncFlatMapExample implements StreamApplication {
    @Override
    public void describe(StreamApplicationDescriptor appDescriptor) {
      Config config = appDescriptor.getConfig();
      KafkaSystemDescriptor kafkaSystemDescriptor = new KafkaSystemDescriptor(TEST_SYSTEM);
      KafkaOutputDescriptor<PageView>
          outputDescriptor = kafkaSystemDescriptor.getOutputDescriptor(NON_GUEST_PAGE_VIEW_STREAM, new NoOpSerde<>());
      OutputStream<PageView> nonGuestPageViewStream = appDescriptor.getOutputStream(outputDescriptor);

      Predicate<PageView> failProcess = (Predicate<PageView> & Serializable) (ignored) -> config.getBoolean(FAIL_PROCESS, false);
      Predicate<PageView> failDownstreamOperator = (Predicate<PageView> & Serializable) (ignored) -> config.getBoolean(FAIL_DOWNSTREAM_OPERATOR, false);
      Supplier<Long> processJitter = (Supplier<Long> & Serializable) () -> config.getLong(PROCESS_JITTER, 100);

      appDescriptor.getInputStream(kafkaSystemDescriptor.getInputDescriptor(PAGE_VIEW_STREAM, new NoOpSerde<PageView>()))
          .flatMapAsync(pageView -> filterGuestPageViews(pageView, failProcess, processJitter))
          .filter(pageView -> filterLoginPageViews(pageView, failDownstreamOperator))
          .sendTo(nonGuestPageViewStream);
    }

    private static CompletionStage<Collection<PageView>> filterGuestPageViews(PageView pageView,
        Predicate<PageView> shouldFailProcess, Supplier<Long> processJitter) {
      CompletableFuture<Collection<PageView>> filteredPageViews = CompletableFuture.supplyAsync(() -> {
        try {
          Thread.sleep(processJitter.get());
        } catch (InterruptedException ex) {
          System.out.println("Interrupted during sleep.");
        }

        return Long.parseLong(pageView.getUserId()) < 1 ? Collections.emptyList() : Collections.singleton(pageView);
      });

      if (shouldFailProcess.test(pageView)) {
        filteredPageViews.completeExceptionally(new ProcessFailureException("Remote service threw an exception"));
      }

      return filteredPageViews;
    }

    private static boolean filterLoginPageViews(PageView pageView, Predicate<PageView> shouldFailProcess) {
      if (shouldFailProcess.test(pageView)) {
        throw new FilterFailureException("Filtering login page views ran into an exception");
      }

      return !LOGIN_PAGE.equals(pageView.getPageId());
    }
  }

  private static class ProcessFailureException extends RuntimeException {
    public ProcessFailureException(String s) {
      super(s);
    }
  }

  private static class FilterFailureException extends RuntimeException {
    public FilterFailureException(String s) {
      super(s);
    }
  }
}
