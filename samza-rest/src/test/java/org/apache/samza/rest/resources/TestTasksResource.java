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
package org.apache.samza.rest.resources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import javax.ws.rs.core.Application;
import javax.ws.rs.core.Response;
import org.apache.samza.config.MapConfig;
import org.apache.samza.rest.SamzaRestApplication;
import org.apache.samza.rest.SamzaRestConfig;
import org.apache.samza.rest.model.Partition;
import org.apache.samza.rest.model.Task;
import org.apache.samza.rest.proxy.task.TaskResourceConfig;
import org.apache.samza.rest.resources.mock.MockJobProxy;
import org.apache.samza.rest.resources.mock.MockResourceFactory;
import org.apache.samza.rest.resources.mock.MockTaskProxy;
import org.apache.samza.rest.resources.mock.MockTaskProxyFactory;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.glassfish.jersey.test.JerseyTest;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestTasksResource extends JerseyTest {
  ObjectMapper objectMapper = SamzaObjectMapper.getObjectMapper();

  @Override
  protected Application configure() {
    Map<String, String> configMap = ImmutableMap.of(TaskResourceConfig.CONFIG_TASK_PROXY_FACTORY,
                                                    MockTaskProxyFactory.class.getName(),
                                                    SamzaRestConfig.CONFIG_REST_RESOURCE_FACTORIES,
                                                    MockResourceFactory.class.getName());
    SamzaRestConfig config = new SamzaRestConfig(new MapConfig(configMap));
    return new SamzaRestApplication(config);
  }

  @Test
  public void testGetTasks()
      throws IOException {
    String requestUrl = String.format("v1/jobs/%s/%s/tasks", "testJobName", "testJobId");
    Response response = target(requestUrl).request().get();
    assertEquals(200, response.getStatus());
    Task[] tasks = objectMapper.readValue(response.readEntity(String.class), Task[].class);
    assertEquals(2, tasks.length);
    List<Partition> partitionList = ImmutableList.of(new Partition(MockTaskProxy.SYSTEM_NAME,
                                                                   MockTaskProxy.STREAM_NAME,
                                                                   MockTaskProxy.PARTITION_ID));

    assertEquals(null, tasks[0].getPreferredHost());
    assertEquals(MockTaskProxy.TASK_1_CONTAINER_ID, tasks[0].getContainerId());
    assertEquals(MockTaskProxy.TASK_1_NAME, tasks[0].getTaskName());
    assertEquals(partitionList, tasks[0].getPartitions());

    assertEquals(null, tasks[1].getPreferredHost());
    assertEquals(MockTaskProxy.TASK_2_CONTAINER_ID, tasks[1].getContainerId());
    assertEquals(MockTaskProxy.TASK_2_NAME, tasks[1].getTaskName());
    assertEquals(partitionList, tasks[1].getPartitions());
  }

  @Test
  public void testGetTasksWithInvalidJobName()
      throws IOException {
    String requestUrl = String.format("v1/jobs/%s/%s/tasks", "BadJobName", MockJobProxy.JOB_INSTANCE_4_ID);
    Response resp = target(requestUrl).request().get();
    assertEquals(400, resp.getStatus());
    final Map<String, String> errorMessage = objectMapper.readValue(resp.readEntity(String.class), new TypeReference<Map<String, String>>() {});
    assertTrue(errorMessage.get("message"), errorMessage.get("message").contains("Invalid arguments for getTasks. "));
    resp.close();
  }

  @Test
  public void testGetTasksWithInvalidJobId()
      throws IOException {
    String requestUrl = String.format("v1/jobs/%s/%s/tasks", MockJobProxy.JOB_INSTANCE_1_NAME, "BadJobId");
    Response resp = target(requestUrl).request().get();
    assertEquals(400, resp.getStatus());
    final Map<String, String> errorMessage = objectMapper.readValue(resp.readEntity(String.class), new TypeReference<Map<String, String>>() {});
    assertTrue(errorMessage.get("message"), errorMessage.get("message").contains("Invalid arguments for getTasks. "));
    resp.close();
  }
}
