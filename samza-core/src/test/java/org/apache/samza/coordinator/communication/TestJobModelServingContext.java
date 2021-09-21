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
package org.apache.samza.coordinator.communication;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class TestJobModelServingContext {
  private JobInfoServingContext jobModelServingContext;

  @Before
  public void setup() {
    this.jobModelServingContext = new JobInfoServingContext();
  }

  @Test
  public void testSetGet() throws IOException {
    // return empty if no job model has been set
    assertFalse(this.jobModelServingContext.getSerializedJobModel().isPresent());

    Config config = new MapConfig(ImmutableMap.of("samza.user.config", "config-value"));
    Map<String, ContainerModel> containerModelMap = ImmutableMap.of("0", new ContainerModel("0",
        ImmutableMap.of(new TaskName("Partition 0"), new TaskModel(new TaskName("Partition 0"),
            ImmutableSet.of(new SystemStreamPartition("system", "stream", new Partition(0))), new Partition(0)))));
    JobModel jobModel = new JobModel(config, containerModelMap);
    this.jobModelServingContext.setJobModel(jobModel);
    Optional<byte[]> serializedJobModel = this.jobModelServingContext.getSerializedJobModel();
    assertTrue(serializedJobModel.isPresent());
    assertEquals(jobModel, SamzaObjectMapper.getObjectMapper().readValue(serializedJobModel.get(), JobModel.class));

    config = new MapConfig(ImmutableMap.of("samza.user.config0", "config-value0"));
    containerModelMap = ImmutableMap.of("0", new ContainerModel("0", ImmutableMap.of(new TaskName("Partition 0"),
        new TaskModel(new TaskName("Partition 0"),
            ImmutableSet.of(new SystemStreamPartition("system0", "stream0", new Partition(0))), new Partition(0)))));
    jobModel = new JobModel(config, containerModelMap);
    this.jobModelServingContext.setJobModel(jobModel);
    serializedJobModel = this.jobModelServingContext.getSerializedJobModel();
    assertTrue(serializedJobModel.isPresent());
    assertEquals(jobModel, SamzaObjectMapper.getObjectMapper().readValue(serializedJobModel.get(), JobModel.class));
  }
}