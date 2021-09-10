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
import org.apache.samza.coordinator.lifecycle.NoOpJobRestartSignalFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestJobCoordinatorConfig {
  @Test
  public void testGetJobRestartSignalFactory() {
    assertEquals(NoOpJobRestartSignalFactory.class.getName(),
        new JobCoordinatorConfig(new MapConfig()).getJobRestartSignalFactory());

    JobCoordinatorConfig jobCoordinatorConfig = new JobCoordinatorConfig(new MapConfig(
        ImmutableMap.of(JobCoordinatorConfig.JOB_RESTART_SIGNAL_FACTORY,
            "org.apache.samza.MyJobRestartSignalFactory")));
    assertEquals("org.apache.samza.MyJobRestartSignalFactory", jobCoordinatorConfig.getJobRestartSignalFactory());
  }

  @Test
  public void testGetUseStaticResourceJobCoordinator() {
    assertFalse(new JobCoordinatorConfig(new MapConfig()).getUseStaticResourceJobCoordinator());

    JobCoordinatorConfig jobCoordinatorConfig = new JobCoordinatorConfig(
        new MapConfig(ImmutableMap.of(JobCoordinatorConfig.USE_STATIC_RESOURCE_JOB_COORDINATOR, "true")));
    assertTrue(jobCoordinatorConfig.getUseStaticResourceJobCoordinator());
  }
}