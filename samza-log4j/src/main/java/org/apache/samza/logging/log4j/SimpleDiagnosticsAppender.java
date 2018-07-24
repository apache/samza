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

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.samza.container.SamzaContainerMetrics;
import org.apache.samza.diagnostics.DiagnosticsExceptionEvent;
import org.apache.samza.metrics.ListGauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Provides an in-memory appender that parses LoggingEvents to filter events relevant to diagnostics.
 * Currently, filters exception related events and update an exception metric ({@link ListGauge}) in
 * {@link SamzaContainerMetrics}.
 *
 * When used inconjunction with {@link org.apache.samza.metrics.reporter.MetricsSnapshotReporter} provides a
 * stream of diagnostics-related events.
 */
public class SimpleDiagnosticsAppender extends AppenderSkeleton {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleDiagnosticsAppender.class);
  private final ListGauge<DiagnosticsExceptionEvent> samzaContainerExceptionMetric;

  public SimpleDiagnosticsAppender(SamzaContainerMetrics samzaContainerMetrics) {
    this.samzaContainerExceptionMetric = samzaContainerMetrics.exceptions();
    this.setName(SimpleDiagnosticsAppender.class.getName());

    // ensure appender is attached only once per JVM (regardless of #containers)
    if (org.apache.log4j.Logger.getRootLogger().getAppender(SimpleDiagnosticsAppender.class.getName()) == null) {
      LOG.info("Attaching diagnostics appender to root logger");
      org.apache.log4j.Logger.getRootLogger().addAppender(this);
    }
  }

  @Override
  protected void append(LoggingEvent loggingEvent) {

    try {
      // if an event with a non-null throwable is received => exception event
      if (loggingEvent.getThrowableInformation() != null) {
        DiagnosticsExceptionEvent diagnosticsExceptionEvent =
            new DiagnosticsExceptionEvent(loggingEvent.timeStamp, loggingEvent.getThrowableInformation().getThrowable(),
                loggingEvent.getProperties());

        samzaContainerExceptionMetric.add(diagnosticsExceptionEvent);
        LOG.debug("Received DiagnosticsExceptionEvent " + diagnosticsExceptionEvent);
      } else {
        LOG.debug("Received non-exception event with message " + loggingEvent.getMessage());
      }
    } catch (Exception e) {
      // blanket catch of all exceptions so as to not impact any job
      LOG.error("Exception in logging event parsing", e);
    }
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

  public static void attach(SamzaContainerMetrics metrics) {
    org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();

    if (rootLogger.getAppender(SimpleDiagnosticsAppender.class.getName()) == null) {
      LOG.info("Starting diagnostics appender.");
      rootLogger.addAppender(new SimpleDiagnosticsAppender(metrics));
    }
  }
}
