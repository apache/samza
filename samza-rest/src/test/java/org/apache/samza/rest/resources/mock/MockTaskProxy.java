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
package org.apache.samza.rest.resources.mock;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.rest.model.Partition;
import org.apache.samza.rest.model.Task;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.rest.proxy.task.SamzaTaskProxy;
import org.apache.samza.rest.proxy.task.TaskResourceConfig;
import org.mockito.Mockito;


public class MockTaskProxy extends SamzaTaskProxy {
  public static final List<Partition> PARTITIONS = ImmutableList.of();

  public static final String TASK_1_NAME = "Task1";
  public static final String TASK_1_CONTAINER_ID = "1";
  public static final String TASK_1_PREFERRED_HOST = "TASK_1_PREFERRED_HOST";
  public static final List<String> TASK_1_STORE_NAMES = ImmutableList.of("Task1Store1", "Task1Store2");

  public static final String TASK_2_NAME = "Task2";
  public static final String TASK_2_CONTAINER_ID = "2";
  public static final String TASK_2_PREFERRED_HOST = "TASK_1_PREFERRED_HOST";
  public static final List<String> TASK_2_STORE_NAMES = ImmutableList.of("Task2Store1", "Task2Store2", "Task2Store3");

  public MockTaskProxy() {
    super(new TaskResourceConfig(new MapConfig()),
          new MockInstallationFinder());
  }

  @Override
  protected CoordinatorStreamSystemConsumer initializeCoordinatorStreamConsumer(JobInstance jobInstance) {
    return Mockito.mock(CoordinatorStreamSystemConsumer.class);
  }

  @Override
  protected List<Task> readTasksFromCoordinatorStream(CoordinatorStreamSystemConsumer consumer) {
    return ImmutableList.of(new Task(TASK_1_PREFERRED_HOST, TASK_1_NAME, TASK_1_CONTAINER_ID, PARTITIONS, TASK_1_STORE_NAMES),
                            new Task(TASK_2_PREFERRED_HOST, TASK_2_NAME, TASK_2_CONTAINER_ID, PARTITIONS, TASK_2_STORE_NAMES));
  }
}
