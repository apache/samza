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

package org.apache.samza.serializers.model;

import static org.junit.Assert.assertEquals;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

public class TestSamzaObjectMapper {
  @Test
  public void testJsonTaskModel() throws Exception {
    ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();
    Map<String, String> configMap = new HashMap<String, String>();
    Set<SystemStreamPartition> ssp = new HashSet<>();
    configMap.put("a", "b");
    Config config = new MapConfig(configMap);
    TaskName taskName = new TaskName("test");
    ssp.add(new SystemStreamPartition("foo", "bar", new Partition(1)));
    TaskModel taskModel = new TaskModel(taskName, ssp, new Partition(2));
    Map<TaskName, TaskModel> tasks = new HashMap<TaskName, TaskModel>();
    tasks.put(taskName, taskModel);
    ContainerModel containerModel = new ContainerModel(1, tasks);
    Map<Integer, ContainerModel> containerMap = new HashMap<Integer, ContainerModel>();
    containerMap.put(Integer.valueOf(1), containerModel);
    JobModel jobModel = new JobModel(config, containerMap);
    String str = mapper.writeValueAsString(jobModel);
    JobModel obj = mapper.readValue(str, JobModel.class);
    assertEquals(jobModel, obj);
  }
}
