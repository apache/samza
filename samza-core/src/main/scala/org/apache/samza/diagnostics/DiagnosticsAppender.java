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

package org.apache.samza.diagnostics;

import java.time.Duration;
import java.util.Arrays;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.metrics.ListGauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides an in-memory appender that parses LoggingEvents to filter events relevant to diagnostics.
 * Currently, filters exception related events and update an exception metric ({@link ListGauge}) in
 * {@link SamzaContainerMetrics}.
 *
 * When used inconjunction with {@link org.apache.samza.metrics.reporter.MetricsSnapshotReporter} provides a
 * stream of diagnostics-related events.s
 */
public class DiagnosticsAppender extends AppenderSkeleton {

  private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());
  private final ListGauge<DiagnosticsExceptionEvent> samzaContainerExceptionMetric;

  private final static Duration DEFAULT_EVICTION_DURATION = Duration.ofMinutes(60); //one hour
  private final static Duration DEFAULT_EVICTION_PERIOD = Duration.ofMinutes(1); //one minute
  private final static int DEFAULT_MAX_NITEMS = 1000; // based on max kafka size of 1 MB

  public DiagnosticsAppender(SamzaContainerMetrics samzaContainerMetrics) {
    this.samzaContainerExceptionMetric = (ListGauge<DiagnosticsExceptionEvent>) samzaContainerMetrics.exception();
    DiagnosticsExceptionEventEvictionPolicy diagnosticsExceptionEventEvictionPolicy =
        new DiagnosticsExceptionEventEvictionPolicy(samzaContainerExceptionMetric, DEFAULT_MAX_NITEMS,
            DEFAULT_EVICTION_DURATION, DEFAULT_EVICTION_PERIOD);
    ((ListGauge<DiagnosticsExceptionEvent>) samzaContainerMetrics.exception()).setEvictionPolicy(
        diagnosticsExceptionEventEvictionPolicy);
  }

  @Override
  protected void append(LoggingEvent loggingEvent) {

    // if an event with a non-null throwable is received => exception event
    if (loggingEvent.getThrowableInformation() != null) {

      DiagnosticsExceptionEvent diagnosticsExceptionEvent =
          new DiagnosticsExceptionEvent(loggingEvent.timeStamp, loggingEvent.getMessage().toString(),
              loggingEvent.getThreadName(),
              Arrays.toString(loggingEvent.getThrowableInformation().getThrowableStrRep()),
              getStackTraceIdentifier(loggingEvent.getThrowableInformation().getThrowable().getStackTrace()));

      samzaContainerExceptionMetric.add(diagnosticsExceptionEvent);
      logger.debug("Received DiagnosticsExceptionEvent " + diagnosticsExceptionEvent);
    } else {
      logger.debug("Received non-exception event with message " + loggingEvent.getMessage());
    }
  }

  private int getStackTraceIdentifier(StackTraceElement[] stackTrace) {
    return Arrays.hashCode(stackTrace);
  }

  @Override
  public void close() {
    // Do nothing.
  }

  /**
   * Returns false since this appender requires no layout.
   * @return false
   */
  @Override
  public boolean requiresLayout() {
    return false;
  }
}
