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
package org.apache.samza.processor;

import junit.framework.Assert;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsReporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

public class TestStreamProcessor {
  SimpleMockJobCoordinatorFactory.SimpleMockJobCoordinator coordinator = null;
  StreamProcessor sp = null;
  @Before
  public void setup() throws Exception {
    Map<String, String> mapConfig = new HashMap<>();
    mapConfig.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.apache.samza.processor.SimpleMockJobCoordinatorFactory");

    sp = new StreamProcessor(
        1, new MapConfig(mapConfig), null, new HashMap<String, MetricsReporter>());
    Field coordinatorField = sp.getClass().getDeclaredField("jobCoordinator");
    coordinatorField.setAccessible(true);
    coordinator =
        (SimpleMockJobCoordinatorFactory.SimpleMockJobCoordinator) coordinatorField.get(sp);
  }

  @After
  public void teardown() {
    coordinator = null;
    sp = null;
  }

  @Test
  public void testStreamProcessorStartAndStop() {
    try {
      sp.start();
      Assert.assertEquals(1, coordinator.startCount);
      sp.start();
      // Guarantees that jobCoordinator is not started again
      Assert.assertEquals(1, coordinator.startCount);
    } catch (Exception e) {
      Assert.fail("Calling start multiple times should not throw an exception! " + e);
    } finally {
      sp.syncStop();
      Assert.assertEquals(1, coordinator.stopCount);
    }
  }

  @Test
  public void testCallingStopBeforeStartIsOk() {
    try {
      sp.syncStop();
      Assert.assertEquals(0, coordinator.stopCount);
      sp.start();
      Assert.assertEquals(1, coordinator.startCount);
    } catch (Exception e) {
      Assert.fail("Calling stop before start should not throw an exception! " + e);
    } finally {
      sp.syncStop();
      Assert.assertEquals(1, coordinator.stopCount);
    }
  }
}

