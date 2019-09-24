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
 *
 */

package org.apache.samza.util;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestDefaultCoordinatorStreamConfigFactory {

  DefaultCoordinatorStreamConfigFactory factory = new DefaultCoordinatorStreamConfigFactory();

  @Test
  public void testBuildCoordinatorStreamConfigWithJobName() {
    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put("job.name", "testName");
    mapConfig.put("job.id", "testId");
    mapConfig.put("job.coordinator.system", "testSamza");
    mapConfig.put("test.only", "nothing");
    mapConfig.put("systems.testSamza.test", "test");

    Config config = factory.buildCoordinatorStreamConfig(new MapConfig(mapConfig));

    assertEquals(config.get("systems.testSamza.test"), "test");
    assertEquals(config.get(JobConfig.JOB_NAME), "testName");
    assertNull(config.get("test.only"));
  }

  @Test(expected = NoSuchElementException.class)
  public void testBuildCoordinatorStreamConfigWithoutJobName() {
    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put("job.id", "testId");
    mapConfig.put("job.coordinator.system", "testSamza");
    mapConfig.put("test.only", "nothing");
    mapConfig.put("systems.testSamza.test", "test");

    factory.buildCoordinatorStreamConfig(new MapConfig(mapConfig));
  }
}
