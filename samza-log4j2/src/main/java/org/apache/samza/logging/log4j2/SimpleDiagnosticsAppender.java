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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.samza.diagnostics.DiagnosticsExceptionEvent;
import org.apache.samza.diagnostics.DiagnosticsManager;

/**
 * Provides an in-memory appender that parses LogEvents to filter events relevant to diagnostics.
 * Currently, filters exception related events and updates the {@link DiagnosticsManager}.
 *
 * When used inconjunction with {@link org.apache.samza.metrics.reporter.MetricsSnapshotReporter} provides a
 * stream of diagnostics-related events.
 */
public class SimpleDiagnosticsAppender extends AbstractAppender {

  // simple object to synchronize root logger attachment
  private static final Object SYNCHRONIZATION_OBJECT = new Object();
  protected final DiagnosticsManager diagnosticsManager;

  public SimpleDiagnosticsAppender(DiagnosticsManager diagnosticsManager) {
    super(SimpleDiagnosticsAppender.class.getName(), null, null);
    this.diagnosticsManager = diagnosticsManager;

    synchronized (SYNCHRONIZATION_OBJECT) {
      attachAppenderToLoggers(this);
    }

    System.out.println("SimpleDiagnosticsAppender initialized ");
  }

  private void attachAppenderToLoggers(Appender appender) {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();

    // ensure appender is attached only once per JVM (regardless of #containers)
    if (config.getRootLogger().getAppenders().get(SimpleDiagnosticsAppender.class.getName()) == null) {
      System.out.println("Attaching diagnostics appender to root logger");
      appender.start();
      config.addAppender(appender);
      for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
        loggerConfig.addAppender(appender, null, null);
      }
    }
  }

  @Override
  public void append(LogEvent logEvent) {
    try {
      // if an event with a non-null throwable is received => exception event
      if (logEvent.getThrown() != null) {
        DiagnosticsExceptionEvent diagnosticsExceptionEvent =
            new DiagnosticsExceptionEvent(logEvent.getTimeMillis(), logEvent.getThrown(),
                logEvent.getContextData().toMap());

        diagnosticsManager.addExceptionEvent(diagnosticsExceptionEvent);
      }
    } catch (Exception e) {
      // blanket catch of all exceptions so as to not impact any job
      System.err.println("Exception in logevent parsing " + e);
    }
  }
}
