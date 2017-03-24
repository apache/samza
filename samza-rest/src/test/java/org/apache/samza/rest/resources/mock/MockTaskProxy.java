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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.rest.proxy.job.JobInstance;
import org.apache.samza.rest.proxy.task.SamzaTaskProxy;
import org.apache.samza.rest.proxy.task.TaskResourceConfig;
import org.apache.samza.system.SystemStreamPartition;


public class MockTaskProxy extends SamzaTaskProxy {
  public static final String SYSTEM_NAME = "testSystem";
  public static final String STREAM_NAME = "testStream";
  public static final Integer PARTITION_ID = 1;
  public static final Set<SystemStreamPartition> SYSTEM_STREAM_PARTITIONS = ImmutableSet.of(
      new SystemStreamPartition(SYSTEM_NAME, STREAM_NAME, new Partition(PARTITION_ID)));

  public static final String TASK_1_NAME = "Task1";
  public static final String TASK_1_CONTAINER_ID = "1";
  public static final Partition CHANGE_LOG_PARTITION = new Partition(0);

  public static final String TASK_2_NAME = "Task2";
  public static final String TASK_2_CONTAINER_ID = "2";

  public MockTaskProxy() {
    super(new TaskResourceConfig(new MapConfig()),
          new MockInstallationFinder());
  }

  @Override
  protected JobModel getJobModel(JobInstance jobInstance) {
    if (jobInstance.getJobId().contains("Bad")
        || jobInstance.getJobName().contains("Bad")) {
      throw new IllegalArgumentException("No tasks found.");
    }
    TaskModel task1Model = new TaskModel(new TaskName(TASK_1_NAME), SYSTEM_STREAM_PARTITIONS, CHANGE_LOG_PARTITION);
    TaskModel task2Model = new TaskModel(new TaskName(TASK_2_NAME), SYSTEM_STREAM_PARTITIONS, CHANGE_LOG_PARTITION);
    ContainerModel task1ContainerModel = new ContainerModel(TASK_1_CONTAINER_ID,
                                                            ImmutableMap.of(new TaskName(TASK_1_NAME),
                                                                            task1Model));
    ContainerModel task2ContainerModel = new ContainerModel(TASK_2_CONTAINER_ID,
                                                            ImmutableMap.of(new TaskName(TASK_2_NAME),
                                                                            task2Model));
    return new JobModel(new MapConfig(), ImmutableMap.of(TASK_1_CONTAINER_ID, task1ContainerModel,
                                                         TASK_2_CONTAINER_ID, task2ContainerModel));
  }
}
