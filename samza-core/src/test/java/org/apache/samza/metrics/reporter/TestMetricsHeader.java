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

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class TestMetricsHeader {
  private static final String JOB_NAME = "job-a";
  private static final String JOB_ID = "id-a";
  private static final String CONTAINER_NAME = "samza-container-0";
  private static final String EXEC_ENV_CONTAINER_ID = "container-12345";
  private static final String SOURCE = "metrics-source";
  private static final String VERSION = "1.2.3";
  private static final String SAMZA_VERSION = "4.5.6";
  private static final String HOST = "host0.a.b.c";
  private static final long TIME = 100;
  private static final long RESET_TIME = 10;

  @Test
  public void testGetAsMap() {
    MetricsHeader metricsHeader =
        new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXEC_ENV_CONTAINER_ID, SOURCE, VERSION, SAMZA_VERSION, HOST,
            TIME, RESET_TIME);
    Map<String, Object> expected = new HashMap<>();
    expected.put("job-name", JOB_NAME);
    expected.put("job-id", JOB_ID);
    expected.put("container-name", CONTAINER_NAME);
    expected.put("exec-env-container-id", EXEC_ENV_CONTAINER_ID);
    expected.put("source", SOURCE);
    expected.put("version", VERSION);
    expected.put("samza-version", SAMZA_VERSION);
    expected.put("host", HOST);
    expected.put("time", TIME);
    expected.put("reset-time", RESET_TIME);
    assertEquals(expected, metricsHeader.getAsMap());
  }

  @Test
  public void testFromMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("job-name", JOB_NAME);
    map.put("job-id", JOB_ID);
    map.put("container-name", CONTAINER_NAME);
    map.put("exec-env-container-id", EXEC_ENV_CONTAINER_ID);
    map.put("source", SOURCE);
    map.put("version", VERSION);
    map.put("samza-version", SAMZA_VERSION);
    map.put("host", HOST);
    map.put("time", TIME);
    map.put("reset-time", RESET_TIME);
    MetricsHeader expected =
        new MetricsHeader(JOB_NAME, JOB_ID, CONTAINER_NAME, EXEC_ENV_CONTAINER_ID, SOURCE, VERSION, SAMZA_VERSION, HOST,
            TIME, RESET_TIME);
    assertEquals(expected, MetricsHeader.fromMap(map));
  }
}
