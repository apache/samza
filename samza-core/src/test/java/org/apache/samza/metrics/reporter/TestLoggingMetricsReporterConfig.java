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
import com.google.common.collect.ImmutableMap;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestLoggingMetricsReporterConfig {
  private static final String REPORTER_NAME = "reporter_name";

  @Test
  public void testGetMetricsToLogRegex() {
    Map<String, String> configMap = ImmutableMap.of("metrics.reporter.reporter_name.log.regex", ".*metric.*");
    assertEquals(".*metric.*",
        new LoggingMetricsReporterConfig(new MapConfig(configMap)).getMetricsToLogRegex(REPORTER_NAME));
  }

  @Test(expected = ConfigException.class)
  public void testGetMetricsToLogRegexMissing() {
    new LoggingMetricsReporterConfig(new MapConfig()).getMetricsToLogRegex(REPORTER_NAME);
  }

  @Test
  public void testGetLoggingIntervalSeconds() {
    assertEquals(60, new LoggingMetricsReporterConfig(new MapConfig()).getLoggingIntervalSeconds(REPORTER_NAME));

    Map<String, String> configMap = ImmutableMap.of("metrics.reporter.reporter_name.logging.interval.seconds", "100");
    assertEquals(100,
        new LoggingMetricsReporterConfig(new MapConfig(configMap)).getLoggingIntervalSeconds(REPORTER_NAME));
  }
}