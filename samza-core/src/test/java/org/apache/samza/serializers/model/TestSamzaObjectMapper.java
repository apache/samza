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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

public class TestSamzaObjectMapper {
  private JobModel jobModel;
  private String jobModelJsonString;
  private ObjectMapper mapper;

  @Before
  public void setup() throws IOException {
    mapper = SamzaObjectMapper.getObjectMapper();

    Map<String, String> configMap = new HashMap<String, String>();
    Set<SystemStreamPartition> ssp = new HashSet<>();
    configMap.put("a", "b");
    Config config = new MapConfig(configMap);
    TaskName taskName = new TaskName("test");
    ssp.add(new SystemStreamPartition("foo", "bar", new Partition(1)));
    TaskModel taskModel = new TaskModel(taskName, ssp, new Partition(2));
    Map<TaskName, TaskModel> tasks = new HashMap<TaskName, TaskModel>();
    tasks.put(taskName, taskModel);
    ContainerModel containerModel = new ContainerModel("1", tasks);
    Map<String, ContainerModel> containerMap = new HashMap<String, ContainerModel>();
    containerMap.put("1", containerModel);
    jobModel = new JobModel(config, containerMap);

    jobModelJsonString = mapper.writeValueAsString(jobModel);
  }

  @Test
  public void testJsonTaskModel() throws Exception {
    ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();

    String str = mapper.writeValueAsString(jobModel);
    JobModel obj = mapper.readValue(str, JobModel.class);
    assertEquals(jobModel, obj);
  }


  @Test
  public void testJobModelStringToObjectMapping() throws IOException {
    String newJobModelString = "{\"config\":{\"a\":\"b\"},\"containers\":{\"1\":{\"processor-id\":\"1\",\"container-id\":-1,\"tasks\":{\"test\":{\"task-name\":\"test\",\"system-stream-partitions\":[{\"system\":\"foo\",\"partition\":1,\"stream\":\"bar\"}],\"changelog-partition\":2}}}},\"max-change-log-stream-partitions\":3,\"all-container-locality\":{\"1\":null}}";
    ObjectMapper mapper = SamzaObjectMapper.getObjectMapper();
    JobModel jobModel = mapper.readValue(newJobModelString, JobModel.class);
    System.out.println("Here");
  }

}
