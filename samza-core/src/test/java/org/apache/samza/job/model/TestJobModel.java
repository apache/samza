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
package org.apache.samza.job.model;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestJobModel {
  @Test
  public void testMaxChangeLogStreamPartitions() {
    Config config = new MapConfig(ImmutableMap.of("a", "b"));
    Map<TaskName, TaskModel> tasksForContainer1 = ImmutableMap.of(
        new TaskName("t1"), new TaskModel(new TaskName("t1"), ImmutableSet.of(), new Partition(0)),
        new TaskName("t2"), new TaskModel(new TaskName("t2"), ImmutableSet.of(), new Partition(1)));
    Map<TaskName, TaskModel> tasksForContainer2 = ImmutableMap.of(
        new TaskName("t3"), new TaskModel(new TaskName("t3"), ImmutableSet.of(), new Partition(2)),
        new TaskName("t4"), new TaskModel(new TaskName("t4"), ImmutableSet.of(), new Partition(3)),
        new TaskName("t5"), new TaskModel(new TaskName("t5"), ImmutableSet.of(), new Partition(4)));
    ContainerModel containerModel1 = new ContainerModel("0", tasksForContainer1);
    ContainerModel containerModel2 = new ContainerModel("1", tasksForContainer2);
    Map<String, ContainerModel> containers = ImmutableMap.of("0", containerModel1, "1", containerModel2);
    JobModel jobModel = new JobModel(config, containers);
    assertEquals(jobModel.getMaxChangeLogStreamPartitions(), 5);
  }
}