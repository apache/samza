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
package org.apache.samza.container.grouper.task;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.MockCoordinatorStreamSystemFactory;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.Partition;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(CoordinatorStreamUtil.class)
public class TestTaskPartitionAssignmentManager {

  private static final String TEST_SYSTEM = "system";
  private static final String TEST_STREAM = "stream";
  private static final Partition PARTITION = new Partition(0);

  private final Config config = new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));
  private final SystemStreamPartition testSystemStreamPartition = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, PARTITION);

  private MockCoordinatorStreamSystemFactory mockCoordinatorStreamSystemFactory;
  private TaskPartitionAssignmentManager taskPartitionAssignmentManager;

  @Before
  public void setup() {
    mockCoordinatorStreamSystemFactory = new MockCoordinatorStreamSystemFactory();
    MockCoordinatorStreamSystemFactory.enableMockConsumerCache();
    PowerMockito.mockStatic(CoordinatorStreamUtil.class);
    when(CoordinatorStreamUtil.getCoordinatorSystemFactory(anyObject())).thenReturn(mockCoordinatorStreamSystemFactory);
    when(CoordinatorStreamUtil.getCoordinatorSystemStream(anyObject())).thenReturn(new SystemStream("test-kafka", "test"));
    when(CoordinatorStreamUtil.getCoordinatorStreamName(anyObject(), anyObject())).thenReturn("test");
    taskPartitionAssignmentManager = new TaskPartitionAssignmentManager(config, new MetricsRegistryMap());
  }

  @After
  public void tearDown() {
    MockCoordinatorStreamSystemFactory.disableMockConsumerCache();
    taskPartitionAssignmentManager.close();
  }

  @Test
  public void testReadAfterWrite() {
    List<String> testTaskNames = ImmutableList.of("test-task1", "test-task2", "test-task3");
    taskPartitionAssignmentManager.writeTaskPartitionAssignment(testSystemStreamPartition, testTaskNames);

    Map<SystemStreamPartition, List<String>> expectedMapping = ImmutableMap.of(testSystemStreamPartition, testTaskNames);
    Map<SystemStreamPartition, List<String>> actualMapping = taskPartitionAssignmentManager.readTaskPartitionAssignments();

    Assert.assertEquals(expectedMapping, actualMapping);
  }

  @Test
  public void testDeleteAfterWrite() {
    List<String> testTaskNames = ImmutableList.of("test-task1", "test-task2", "test-task3");
    taskPartitionAssignmentManager.writeTaskPartitionAssignment(testSystemStreamPartition, testTaskNames);

    Map<SystemStreamPartition, List<String>> actualMapping = taskPartitionAssignmentManager.readTaskPartitionAssignments();
    Assert.assertEquals(1, actualMapping.size());

    taskPartitionAssignmentManager.delete(ImmutableList.of(testSystemStreamPartition));

    actualMapping = taskPartitionAssignmentManager.readTaskPartitionAssignments();
    Assert.assertEquals(0, actualMapping.size());
  }

  @Test
  public void testReadPartitionAssignments() {
    SystemStreamPartition testSystemStreamPartition1 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, PARTITION);
    List<String> testTaskNames1 = ImmutableList.of("test-task1", "test-task2", "test-task3");
    SystemStreamPartition testSystemStreamPartition2 = new SystemStreamPartition(TEST_SYSTEM, "stream-2", PARTITION);
    List<String> testTaskNames2 = ImmutableList.of("test-task4", "test-task5");
    SystemStreamPartition testSystemStreamPartition3 = new SystemStreamPartition(TEST_SYSTEM, "stream-3", PARTITION);
    List<String> testTaskNames3 = ImmutableList.of("test-task6", "test-task7", "test-task8");

    taskPartitionAssignmentManager.writeTaskPartitionAssignment(testSystemStreamPartition1, testTaskNames1);
    taskPartitionAssignmentManager.writeTaskPartitionAssignment(testSystemStreamPartition2, testTaskNames2);
    taskPartitionAssignmentManager.writeTaskPartitionAssignment(testSystemStreamPartition3, testTaskNames3);

    Map<SystemStreamPartition, List<String>> expectedMapping = ImmutableMap.of(testSystemStreamPartition1, testTaskNames1,
            testSystemStreamPartition2, testTaskNames2, testSystemStreamPartition3, testTaskNames3);
    Map<SystemStreamPartition, List<String>> actualMapping = taskPartitionAssignmentManager.readTaskPartitionAssignments();

    Assert.assertEquals(expectedMapping, actualMapping);
  }

  @Test
  public void testMultipleUpdatesReturnsTheMostRecentValue() {
    List<String> testTaskNames1 = ImmutableList.of("test-task1", "test-task2", "test-task3");

    taskPartitionAssignmentManager.writeTaskPartitionAssignment(testSystemStreamPartition, testTaskNames1);

    List<String> testTaskNames2 = ImmutableList.of("test-task4", "test-task5");
    taskPartitionAssignmentManager.writeTaskPartitionAssignment(testSystemStreamPartition, testTaskNames2);

    List<String> testTaskNames3 = ImmutableList.of("test-task6", "test-task7", "test-task8");
    taskPartitionAssignmentManager.writeTaskPartitionAssignment(testSystemStreamPartition, testTaskNames3);

    Map<SystemStreamPartition, List<String>> expectedMapping = ImmutableMap.of(testSystemStreamPartition, testTaskNames3);
    Map<SystemStreamPartition, List<String>> actualMapping = taskPartitionAssignmentManager.readTaskPartitionAssignments();
    Assert.assertEquals(expectedMapping, actualMapping);
  }
}
