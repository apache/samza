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

import java.util.Optional;
import com.google.common.collect.ImmutableMap;
import org.apache.samza.coordinator.lifecycle.NoOpJobRestartSignalFactory;
import org.apache.samza.zk.ZkJobCoordinatorFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class TestJobCoordinatorConfig {
  @Test
  public void getJobCoordinatorFactoryClassName() {
    assertEquals(ZkJobCoordinatorFactory.class.getName(),
        new JobCoordinatorConfig(new MapConfig()).getJobCoordinatorFactoryClassName());

    JobCoordinatorConfig jobCoordinatorConfig =
        new JobCoordinatorConfig(new MapConfig(ImmutableMap.of(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "")));
    assertEquals(ZkJobCoordinatorFactory.class.getName(), jobCoordinatorConfig.getJobCoordinatorFactoryClassName());

    jobCoordinatorConfig = new JobCoordinatorConfig(new MapConfig(
        ImmutableMap.of(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.custom.MyJobCoordinatorFactory")));
    assertEquals("org.custom.MyJobCoordinatorFactory", jobCoordinatorConfig.getJobCoordinatorFactoryClassName());
  }

  @Test
  public void getOptionalJobCoordinatorFactoryClassName() {
    assertFalse(new JobCoordinatorConfig(new MapConfig()).getOptionalJobCoordinatorFactoryClassName().isPresent());

    JobCoordinatorConfig jobCoordinatorConfig =
        new JobCoordinatorConfig(new MapConfig(ImmutableMap.of(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "")));
    assertFalse(jobCoordinatorConfig.getOptionalJobCoordinatorFactoryClassName().isPresent());

    jobCoordinatorConfig = new JobCoordinatorConfig(new MapConfig(
        ImmutableMap.of(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, "org.custom.MyJobCoordinatorFactory")));
    assertEquals(Optional.of("org.custom.MyJobCoordinatorFactory"),
        jobCoordinatorConfig.getOptionalJobCoordinatorFactoryClassName());
  }

  @Test
  public void testGetJobRestartSignalFactory() {
    assertEquals(NoOpJobRestartSignalFactory.class.getName(),
        new JobCoordinatorConfig(new MapConfig()).getJobRestartSignalFactory());

    JobCoordinatorConfig jobCoordinatorConfig = new JobCoordinatorConfig(new MapConfig(
        ImmutableMap.of(JobCoordinatorConfig.JOB_RESTART_SIGNAL_FACTORY,
            "org.apache.samza.MyJobRestartSignalFactory")));
    assertEquals("org.apache.samza.MyJobRestartSignalFactory", jobCoordinatorConfig.getJobRestartSignalFactory());
  }
}