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

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import com.google.common.annotations.VisibleForTesting;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.Metric;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.MetricsVisitor;
import org.apache.samza.metrics.ReadableMetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of {@link MetricsReporter} which logs metrics which match a regex.
 * The regex is checked against "[source name]-[group name]-[metric name]".
 */
public class LoggingMetricsReporter implements MetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingMetricsReporter.class);
  /**
   * First part is source, second part is group name, third part is metric name
   */
  private static final String FULL_METRIC_FORMAT = "%s-%s-%s";

  private final ScheduledExecutorService scheduledExecutorService;
  private final Pattern metricsToLog;
  private final long loggingIntervalSeconds;
  private final Queue<Runnable> loggingTasks = new ConcurrentLinkedQueue<>();

  /**
   * @param scheduledExecutorService executes the logging tasks
   * @param metricsToLog Only log the metrics which match this regex. The strings for matching against this metric are
   *                     constructed by concatenating source name, group name, and metric name, delimited by dashes.
   * @param loggingIntervalSeconds interval at which to log metrics
   */
  public LoggingMetricsReporter(ScheduledExecutorService scheduledExecutorService, Pattern metricsToLog,
      long loggingIntervalSeconds) {
    this.scheduledExecutorService = scheduledExecutorService;
    this.metricsToLog = metricsToLog;
    this.loggingIntervalSeconds = loggingIntervalSeconds;
  }

  @Override
  public void start() {
    this.scheduledExecutorService.scheduleAtFixedRate(() -> this.loggingTasks.forEach(Runnable::run),
        this.loggingIntervalSeconds, this.loggingIntervalSeconds, TimeUnit.SECONDS);
  }

  @Override
  public void register(String source, ReadableMetricsRegistry registry) {
    this.loggingTasks.add(buildLoggingTask(source, registry));
  }

  @Override
  public void stop() {
    this.scheduledExecutorService.shutdown();
    try {
      this.scheduledExecutorService.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while shutting down executor", e);
    }
    if (!this.scheduledExecutorService.isTerminated()) {
      LOG.warn("Unable to shutdown executor");
    }
  }

  /**
   * VisibleForTesting so that the logging call can be verified in unit tests.
   */
  @VisibleForTesting
  void doLog(String logString) {
    LOG.info(logString);
  }

  private Runnable buildLoggingTask(String source, ReadableMetricsRegistry registry) {
    return () -> {
      for (String group : registry.getGroups()) {
        for (Map.Entry<String, Metric> metricGroupEntry : registry.getGroup(group).entrySet()) {
          metricGroupEntry.getValue().visit(new MetricsVisitor() {
            @Override
            public void counter(Counter counter) {
              logMetric(source, group, counter.getName(), counter.getCount());
            }

            @Override
            public <T> void gauge(Gauge<T> gauge) {
              logMetric(source, group, gauge.getName(), gauge.getValue());
            }

            @Override
            public void timer(Timer timer) {
              logMetric(source, group, timer.getName(), timer.getSnapshot().getAverage());
            }
          });
        }
      }
    };
  }

  private <T> void logMetric(String source, String group, String metricName, T value) {
    String fullMetricName = String.format(FULL_METRIC_FORMAT, source, group, metricName);
    if (this.metricsToLog.matcher(fullMetricName).matches()) {
      doLog(String.format("Metric: %s, Value: %s", fullMetricName, value));
    }
  }
}
