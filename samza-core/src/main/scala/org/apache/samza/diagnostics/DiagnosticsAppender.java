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

  public DiagnosticsAppender(SamzaContainerMetrics samzaContainerMetrics) {
    this.samzaContainerExceptionMetric = (ListGauge<DiagnosticsExceptionEvent>) samzaContainerMetrics.exceptions();
  }

  @Override
  protected void append(LoggingEvent loggingEvent) {

    try {

      // if an event with a non-null throwable is received => exception event
      if (loggingEvent.getThrowableInformation() != null) {

        Throwable throwable = loggingEvent.getThrowableInformation().getThrowable();
        Throwable throwableCause = loggingEvent.getThrowableInformation().getThrowable().getCause();

        String throwableClassName = (throwable == null) ? "" : throwable.getClass().getName();
        String throwableCauseClassName = (throwableCause == null) ? "" : throwableCause.getClass().getName();
        String throwableCauseMessage = (throwableCause == null) ? "" : throwableCause.getMessage();

        DiagnosticsExceptionEvent diagnosticsExceptionEvent =
            new DiagnosticsExceptionEvent(loggingEvent.timeStamp, throwableClassName,
                loggingEvent.getMessage().toString(), throwableCauseClassName, throwableCauseMessage,
                Arrays.toString(loggingEvent.getThrowableInformation().getThrowableStrRep()),
                getStackTraceIdentifier(loggingEvent.getThrowableInformation().getThrowable().getStackTrace()));

        samzaContainerExceptionMetric.add(diagnosticsExceptionEvent);
        logger.debug("Received DiagnosticsExceptionEvent " + diagnosticsExceptionEvent);
      } else {
        logger.debug("Received non-exception event with message " + loggingEvent.getMessage());
      }
    } catch (Exception e) {
      // blanket catch of all exceptions so as to not impact any job
      logger.error("Exception in logging event parsing", e);
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
