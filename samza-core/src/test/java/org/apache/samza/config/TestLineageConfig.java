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
package org.apache.samza.config;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestLineageConfig {

  @Test
  public void testGetLineageFactoryClassName() {
    String expectedLineageFactory = "org.apache.samza.MockLineageFactory";
    MapConfig config =
        new MapConfig(ImmutableMap.of(LineageConfig.LINEAGE_FACTORY, expectedLineageFactory));
    LineageConfig lineageConfig = new LineageConfig(config);
    assertTrue(lineageConfig.getLineageFactoryClassName().isPresent());
    assertEquals(expectedLineageFactory, lineageConfig.getLineageFactoryClassName().get());
  }

  @Test
  public void testGetLineageReporterFactoryClassName() {
    String expectedLineageReporterFactory = "org.apache.samza.MockLineageReporterFactory";
    MapConfig config =
        new MapConfig(ImmutableMap.of(LineageConfig.LINEAGE_REPORTER_FACTORY, expectedLineageReporterFactory));
    LineageConfig lineageConfig = new LineageConfig(config);
    assertTrue(lineageConfig.getLineageReporterFactoryClassName().isPresent());
    assertEquals(expectedLineageReporterFactory, lineageConfig.getLineageReporterFactoryClassName().get());
  }

  @Test
  public void testGetLineageReporterStreamName() {
    String expectedLineageReporterStream = "kafka.foo";
    MapConfig config =
        new MapConfig(ImmutableMap.of(LineageConfig.LINEAGE_REPORTER_STREAM, expectedLineageReporterStream));
    LineageConfig lineageConfig = new LineageConfig(config);
    assertTrue(lineageConfig.getLineageReporterStreamName().isPresent());
    assertEquals(expectedLineageReporterStream, lineageConfig.getLineageReporterStreamName().get());
  }
}
