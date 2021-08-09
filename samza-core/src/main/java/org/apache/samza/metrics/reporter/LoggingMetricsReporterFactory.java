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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsReporter;
import org.apache.samza.metrics.MetricsReporterFactory;


/**
 * Creates a {@link MetricsReporter} which logs metrics and their values.
 * This can be used to access metric values when no other external metrics system is available.
 */
public class LoggingMetricsReporterFactory implements MetricsReporterFactory {
  @Override
  public MetricsReporter getMetricsReporter(String name, String processorId, Config config) {
    ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setNameFormat("Samza LoggingMetricsReporter Thread-%d").setDaemon(true).build());
    LoggingMetricsReporterConfig loggingMetricsReporterConfig = new LoggingMetricsReporterConfig(config);
    Pattern metricsToLog = Pattern.compile(loggingMetricsReporterConfig.getMetricsToLogRegex(name));
    return new LoggingMetricsReporter(scheduledExecutorService, metricsToLog,
        loggingMetricsReporterConfig.getLoggingIntervalSeconds(name));
  }
}
