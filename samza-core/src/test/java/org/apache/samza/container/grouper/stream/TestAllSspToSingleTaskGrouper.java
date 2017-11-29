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

package org.apache.samza.container.grouper.stream;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.*;


public class TestAllSspToSingleTaskGrouper {
  private SystemStreamPartition aa0 = new SystemStreamPartition("SystemA", "StreamA", new Partition(0));
  private SystemStreamPartition aa1 = new SystemStreamPartition("SystemA", "StreamA", new Partition(1));
  private SystemStreamPartition aa2 = new SystemStreamPartition("SystemA", "StreamA", new Partition(2));
  private SystemStreamPartition ab0 = new SystemStreamPartition("SystemA", "StreamB", new Partition(0));
  private AllSspToSingleTaskGrouperFactory grouperFactory = new AllSspToSingleTaskGrouperFactory();

  @Test
  public void testLocalStreamGroupedCorrectlyForYarn() {
    HashSet<SystemStreamPartition> allSSPs = new HashSet<>();
    HashMap<String, String> configMap = new HashMap<>();

    configMap.put("yarn.container.count", "2");

    Config config = new MapConfig(configMap);

    SystemStreamPartitionGrouper grouper = grouperFactory.getSystemStreamPartitionGrouper(config);

    Collections.addAll(allSSPs, aa0, aa1, aa2, ab0);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(allSSPs);
    Map<TaskName, Set<SystemStreamPartition>> expectedResult = new HashMap<>();

    HashSet<SystemStreamPartition> partitions = new HashSet<>();
    partitions.add(aa0);
    partitions.add(aa1);
    partitions.add(aa2);
    partitions.add(ab0);
    expectedResult.put(new TaskName("Task-0"), partitions);
    expectedResult.put(new TaskName("Task-1"), partitions);

    assertEquals(expectedResult, result);
  }

  @Test
  public void testLocalStreamGroupedCorrectlyForPassthru() {
    HashSet<SystemStreamPartition> allSSPs = new HashSet<>();
    HashMap<String, String> configMap = new HashMap<>();

    configMap.put("processor.id", "1");
    configMap.put("job.coordinator.factory", "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");

    Config config = new MapConfig(configMap);

    SystemStreamPartitionGrouper grouper = grouperFactory.getSystemStreamPartitionGrouper(config);

    Collections.addAll(allSSPs, aa0, aa1, aa2, ab0);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(allSSPs);
    Map<TaskName, Set<SystemStreamPartition>> expectedResult = new HashMap<>();

    HashSet<SystemStreamPartition> partitions = new HashSet<>();
    partitions.add(aa0);
    partitions.add(aa1);
    partitions.add(aa2);
    partitions.add(ab0);
    expectedResult.put(new TaskName("Task-1"), partitions);

    assertEquals(expectedResult, result);
  }

  @Test
  public void testLocalStreamWithoutProcessorIdConfigForPassthru() {
    HashSet<SystemStreamPartition> allSSPs = new HashSet<>();
    HashMap<String, String> configMap = new HashMap<>();

    configMap.put("job.coordinator.factory", "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
    Config config = new MapConfig(configMap);

    SystemStreamPartitionGrouper grouper = grouperFactory.getSystemStreamPartitionGrouper(config);

    Collections.addAll(allSSPs, aa0, aa1);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(allSSPs);
    Map<TaskName, Set<SystemStreamPartition>> expectedResult = new HashMap<>();

    HashSet<SystemStreamPartition> partitions = new HashSet<>();
    partitions.add(aa0);
    partitions.add(aa1);
    expectedResult.put(new TaskName("Task-1"), partitions);

    assertEquals(result.size(), 1);
    assertArrayEquals(result.values().toArray(), expectedResult.values().toArray());
  }

  @Test(expected = SamzaException.class)
  public void testLocalStreamWithEmptySsps() {
    HashSet<SystemStreamPartition> allSSPs = new HashSet<>();
    HashMap<String, String> configMap = new HashMap<>();

    configMap.put("job.coordinator.factory", "org.apache.samza.standalone.PassthroughJobCoordinatorFactory");
    Config config = new MapConfig(configMap);

    SystemStreamPartitionGrouper grouper = grouperFactory.getSystemStreamPartitionGrouper(config);

    grouper.group(allSSPs);
  }

  @Test(expected = ConfigException.class)
  public void testLocalStreamWithBroadcastStream() {
    HashMap<String, String> configMap = new HashMap<>();

    configMap.put("task.broadcast.inputs", "test.stream#0");
    Config config = new MapConfig(configMap);

    grouperFactory.getSystemStreamPartitionGrouper(config);
  }

}
