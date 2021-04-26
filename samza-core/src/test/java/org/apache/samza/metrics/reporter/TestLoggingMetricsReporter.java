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

package org.apache.samza.metrics.reporter;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.MetricsVisitor;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.metrics.Snapshot;
import org.apache.samza.metrics.Timer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestLoggingMetricsReporter {
  private static final long LOGGING_INTERVAL_SECONDS = 15;
  private static final String COUNTER_NAME = "counter_name";
  private static final long COUNTER_VALUE = 10;
  private static final String GAUGE_NAME = "gauge_name";
  private static final double GAUGE_VALUE = 20.0;
  private static final String TIMER_NAME = "timer_name";
  private static final double TIMER_VALUE = 30.0;
  private static final Pattern DEFAULT_PATTERN = Pattern.compile(".*_name");
  private static final String GROUP_NAME = "group_name";
  private static final String SOURCE_NAME = "source_name";

  @Mock
  private ScheduledExecutorService scheduledExecutorService;
  @Mock
  private ReadableMetricsRegistry readableMetricsRegistry;
  @Mock
  private Counter counter;
  @Mock
  private Gauge<Double> gauge;
  @Mock
  private Timer timer;
  @Mock
  private Snapshot timerSnapshot;

  private LoggingMetricsReporter loggingMetricsReporter;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    when(this.scheduledExecutorService.scheduleAtFixedRate(any(), eq(LOGGING_INTERVAL_SECONDS),
        eq(LOGGING_INTERVAL_SECONDS), eq(TimeUnit.SECONDS))).thenAnswer((Answer<Void>) invocation -> {
          Runnable runnable = invocation.getArgumentAt(0, Runnable.class);
          runnable.run();
          return null;
        });

    when(this.counter.getName()).thenReturn(COUNTER_NAME);
    when(this.counter.getCount()).thenReturn(COUNTER_VALUE);
    doAnswer(invocation -> {
      invocation.getArgumentAt(0, MetricsVisitor.class).counter(this.counter);
      return null;
    }).when(this.counter).visit(any());

    when(this.gauge.getName()).thenReturn(GAUGE_NAME);
    when(this.gauge.getValue()).thenReturn(GAUGE_VALUE);
    doAnswer(invocation -> {
      invocation.getArgumentAt(0, MetricsVisitor.class).gauge(this.gauge);
      return null;
    }).when(this.gauge).visit(any());

    when(this.timer.getName()).thenReturn(TIMER_NAME);
    when(this.timer.getSnapshot()).thenReturn(this.timerSnapshot);
    doAnswer(invocation -> {
      invocation.getArgumentAt(0, MetricsVisitor.class).timer(this.timer);
      return null;
    }).when(this.timer).visit(any());
    when(this.timerSnapshot.getAverage()).thenReturn(TIMER_VALUE);

    this.loggingMetricsReporter =
        spy(new LoggingMetricsReporter(this.scheduledExecutorService, DEFAULT_PATTERN, LOGGING_INTERVAL_SECONDS));
  }

  @Test
  public void testMetricTypes() {
    when(this.readableMetricsRegistry.getGroups()).thenReturn(Collections.singleton(GROUP_NAME));
    Map<String, Metric> metrics =
        ImmutableMap.of(COUNTER_NAME, this.counter, GAUGE_NAME, this.gauge, TIMER_NAME, this.timer);
    when(this.readableMetricsRegistry.getGroup(GROUP_NAME)).thenReturn(metrics);

    this.loggingMetricsReporter.register(SOURCE_NAME, this.readableMetricsRegistry);
    this.loggingMetricsReporter.start();

    verify(this.loggingMetricsReporter).doLog("Metric: source_name-group_name-counter_name, Value: 10");
    verify(this.loggingMetricsReporter).doLog("Metric: source_name-group_name-gauge_name, Value: 20.0");
    verify(this.loggingMetricsReporter).doLog("Metric: source_name-group_name-timer_name, Value: 30.0");
  }

  @Test
  public void testMultipleRegister() {
    when(this.readableMetricsRegistry.getGroups()).thenReturn(Collections.singleton(GROUP_NAME));
    when(this.readableMetricsRegistry.getGroup(GROUP_NAME)).thenReturn(ImmutableMap.of(COUNTER_NAME, this.counter));
    ReadableMetricsRegistry otherRegistry = mock(ReadableMetricsRegistry.class);
    String otherGroupName = "other_group";
    when(otherRegistry.getGroups()).thenReturn(Collections.singleton(otherGroupName));
    when(otherRegistry.getGroup(otherGroupName)).thenReturn(ImmutableMap.of(GAUGE_NAME, this.gauge));

    this.loggingMetricsReporter.register(SOURCE_NAME, this.readableMetricsRegistry);
    this.loggingMetricsReporter.register("other_source", otherRegistry);
    this.loggingMetricsReporter.start();

    verify(this.loggingMetricsReporter).doLog("Metric: source_name-group_name-counter_name, Value: 10");
    verify(this.loggingMetricsReporter).doLog("Metric: other_source-other_group-gauge_name, Value: 20.0");
  }

  @Test
  public void testFiltering() {
    Pattern countersOnly = Pattern.compile(".*counter.*");
    this.loggingMetricsReporter =
        spy(new LoggingMetricsReporter(this.scheduledExecutorService, countersOnly, LOGGING_INTERVAL_SECONDS));

    when(this.readableMetricsRegistry.getGroups()).thenReturn(Collections.singleton(GROUP_NAME));
    Map<String, Metric> metrics = ImmutableMap.of(COUNTER_NAME, this.counter, GAUGE_NAME, this.gauge);
    when(this.readableMetricsRegistry.getGroup(GROUP_NAME)).thenReturn(metrics);

    this.loggingMetricsReporter.register(SOURCE_NAME, this.readableMetricsRegistry);
    this.loggingMetricsReporter.start();

    ArgumentCaptor<String> logs = ArgumentCaptor.forClass(String.class);
    verify(this.loggingMetricsReporter).doLog(logs.capture());
    assertEquals(Collections.singletonList("Metric: source_name-group_name-counter_name, Value: 10"),
        logs.getAllValues());
  }

  @Test
  public void testNewMetricsAfterRegister() {
    when(this.readableMetricsRegistry.getGroups()).thenReturn(Collections.singleton(GROUP_NAME));
    // first round of logging has one metric (counter only), second call has two (counter and gauge)
    when(this.readableMetricsRegistry.getGroup(GROUP_NAME)).thenReturn(ImmutableMap.of(COUNTER_NAME, this.counter))
        .thenReturn(ImmutableMap.of(COUNTER_NAME, this.counter, GAUGE_NAME, this.gauge));

    // capture the logging task so it can be directly executed by the test
    ArgumentCaptor<Runnable> loggingRunnable = ArgumentCaptor.forClass(Runnable.class);
    when(this.scheduledExecutorService.scheduleAtFixedRate(loggingRunnable.capture(), eq(LOGGING_INTERVAL_SECONDS),
        eq(LOGGING_INTERVAL_SECONDS), eq(TimeUnit.SECONDS))).thenReturn(null);

    this.loggingMetricsReporter.register(SOURCE_NAME, this.readableMetricsRegistry);
    this.loggingMetricsReporter.start();

    // simulate first scheduled execution of logging task
    loggingRunnable.getValue().run();
    String expectedCounterLog = "Metric: source_name-group_name-counter_name, Value: 10";
    // only should get log for counter for the first call
    verify(this.loggingMetricsReporter).doLog(expectedCounterLog);
    String expectedGaugeLog = "Metric: source_name-group_name-gauge_name, Value: 20.0";
    verify(this.loggingMetricsReporter, never()).doLog(expectedGaugeLog);

    // simulate second scheduled execution of logging task
    loggingRunnable.getValue().run();
    // should get second log for counter, first log for gauge
    verify(this.loggingMetricsReporter, times(2)).doLog(expectedCounterLog);
    verify(this.loggingMetricsReporter).doLog(expectedGaugeLog);
  }
}