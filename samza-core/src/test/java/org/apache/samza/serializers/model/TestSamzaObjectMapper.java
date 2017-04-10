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

import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestSamzaObjectMapper {
  private JobModel jobModel;

  @Before
  public void setup() throws IOException {
    Map<String, String> configMap = new HashMap<String, String>();
    Set<SystemStreamPartition> ssp = new HashSet<>();
    configMap.put("a", "b");
    Config config = new MapConfig(configMap);
    TaskName taskName = new TaskName("test");
    ssp.add(new SystemStreamPartition("foo", "bar", new Partition(1)));
    TaskModel taskModel = new TaskModel(taskName, ssp, new Partition(2));
    Map<TaskName, TaskModel> tasks = new HashMap<TaskName, TaskModel>();
    tasks.put(taskName, taskModel);
    ContainerModel containerModel = new ContainerModel("1", 1, tasks);
    Map<String, ContainerModel> containerMap = new HashMap<String, ContainerModel>();
    containerMap.put("1", containerModel);
    jobModel = new JobModel(config, containerMap);
  }

  @Test
  public void testJsonTaskModel() throws Exception {
    ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();

    String str = mapper.writeValueAsString(jobModel);
    JobModel obj = mapper.readValue(str, JobModel.class);
    assertEquals(jobModel, obj);
  }

  /**
   * Critical test to guarantee compatibility between samza 0.12 container models and 0.13+
   *
   * Samza 0.12 contains only "container-id" (integer) in the ContainerModel. "processor-id" (String) is added in 0.13.
   * When serializing, we serialize both the fields in 0.13. Deserialization correctly handles the fields in 0.13.
   */
  @Test
  public void testContainerModelCompatible() {
    try {
      String newJobModelString = "{\"config\":{\"a\":\"b\"},\"containers\":{\"1\":{\"processor-id\":\"1\",\"container-id\":1,\"tasks\":{\"test\":{\"task-name\":\"test\",\"system-stream-partitions\":[{\"system\":\"foo\",\"partition\":1,\"stream\":\"bar\"}],\"changelog-partition\":2}}}},\"max-change-log-stream-partitions\":3,\"all-container-locality\":{\"1\":null}}";
      ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();
      JobModel jobModel = mapper.readValue(newJobModelString, JobModel.class);

      String oldJobModelString = "{\"config\":{\"a\":\"b\"},\"containers\":{\"1\":{\"container-id\":1,\"tasks\":{\"test\":{\"task-name\":\"test\",\"system-stream-partitions\":[{\"system\":\"foo\",\"partition\":1,\"stream\":\"bar\"}],\"changelog-partition\":2}}}},\"max-change-log-stream-partitions\":3,\"all-container-locality\":{\"1\":null}}";
      ObjectMapper mapper1 = SamzaObjectMapper.getObjectMapper();
      JobModel jobModel1 = mapper1.readValue(oldJobModelString, JobModel.class);

      Assert.assertEquals(jobModel, jobModel1);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
