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
import org.apache.samza.diagnostics.DiagnosticsExceptionEvent;
import org.apache.samza.diagnostics.DiagnosticsManager;


/**
 * Provides an in-memory appender that parses LoggingEvents to filter events relevant to diagnostics.
 * Currently, filters exception related events and updates the {@link DiagnosticsManager}.
 *
 * When used inconjunction with {@link org.apache.samza.metrics.reporter.MetricsSnapshotReporter} provides a
 * stream of diagnostics-related events.
 */
public class SimpleDiagnosticsAppender extends AppenderSkeleton {

  // simple object to synchronize root logger attachment
  private static final Object SYNCHRONIZATION_OBJECT = new Object();
  protected final DiagnosticsManager diagnosticsManager;

  /**
   * A simple log4j1.2.* appender, which attaches itself to the root logger.
   * Attachment to the root logger is thread safe.
   */
  public SimpleDiagnosticsAppender(DiagnosticsManager diagnosticsManager) {
    this.diagnosticsManager = diagnosticsManager;
    this.setName(SimpleDiagnosticsAppender.class.getName());

    synchronized (SYNCHRONIZATION_OBJECT) {
      this.attachAppenderToRootLogger();
    }
  }

  private void attachAppenderToRootLogger() {
    // ensure appender is attached only once per JVM (regardless of #containers)
    if (org.apache.log4j.Logger.getRootLogger().getAppender(SimpleDiagnosticsAppender.class.getName()) == null) {
      System.out.println("Attaching diagnostics appender to root logger");
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

        diagnosticsManager.addExceptionEvent(diagnosticsExceptionEvent);
      }
    } catch (Exception e) {
      // blanket catch of all exceptions so as to not impact any job
      System.err.println("Exception in logging event parsing " + e);
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
}
