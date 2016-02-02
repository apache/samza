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

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

public class TestGroupBySystemStreamPartition {
  SystemStreamPartition aa0 = new SystemStreamPartition("SystemA", "StreamA", new Partition(0));
  SystemStreamPartition aa1 = new SystemStreamPartition("SystemA", "StreamA", new Partition(1));
  SystemStreamPartition aa2 = new SystemStreamPartition("SystemA", "StreamA", new Partition(2));
  SystemStreamPartition ac0 = new SystemStreamPartition("SystemA", "StreamB", new Partition(0));

  @Test
  public void testLocalStreamGroupedCorrectly() {
    HashSet<SystemStreamPartition> allSSPs = new HashSet<SystemStreamPartition>();

    GroupBySystemStreamPartition grouper = new GroupBySystemStreamPartition();
    Map<TaskName, Set<SystemStreamPartition>> emptyResult = grouper.group(allSSPs);
    assertTrue(emptyResult.isEmpty());

    Collections.addAll(allSSPs, aa0, aa1, aa2, ac0);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(allSSPs);
    Map<TaskName, Set<SystemStreamPartition>> expectedResult = new HashMap<TaskName, Set<SystemStreamPartition>>();

    HashSet<SystemStreamPartition> partitionaa0 = new HashSet<SystemStreamPartition>();
    partitionaa0.add(aa0);
    expectedResult.put(new TaskName(aa0.toString()), partitionaa0);

    HashSet<SystemStreamPartition> partitionaa1 = new HashSet<SystemStreamPartition>();
    partitionaa1.add(aa1);
    expectedResult.put(new TaskName(aa1.toString()), partitionaa1);

    HashSet<SystemStreamPartition> partitionaa2 = new HashSet<SystemStreamPartition>();
    partitionaa2.add(aa2);
    expectedResult.put(new TaskName(aa2.toString()), partitionaa2);

    HashSet<SystemStreamPartition> partitionac0 = new HashSet<SystemStreamPartition>();
    partitionac0.add(ac0);
    expectedResult.put(new TaskName(ac0.toString()), partitionac0);

    assertEquals(expectedResult, result);
  }

  @Test
  public void testBroadcastStreamGroupedCorrectly() {
    HashMap<String, String> configMap = new HashMap<String, String>();
    configMap.put("task.broadcast.inputs", "SystemA.StreamA#0");
    Config config = new MapConfig(configMap);

    HashSet<SystemStreamPartition> allSSPs = new HashSet<SystemStreamPartition>();
    Collections.addAll(allSSPs, aa0, aa1, aa2, ac0);
    GroupBySystemStreamPartition grouper = new GroupBySystemStreamPartition(config);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(allSSPs);

    Map<TaskName, Set<SystemStreamPartition>> expectedResult = new HashMap<TaskName, Set<SystemStreamPartition>>();

    HashSet<SystemStreamPartition> partitionaa1 = new HashSet<SystemStreamPartition>();
    partitionaa1.add(aa1);
    partitionaa1.add(aa0);
    expectedResult.put(new TaskName(aa1.toString()), partitionaa1);

    HashSet<SystemStreamPartition> partitionaa2 = new HashSet<SystemStreamPartition>();
    partitionaa2.add(aa2);
    partitionaa2.add(aa0);
    expectedResult.put(new TaskName(aa2.toString()), partitionaa2);

    HashSet<SystemStreamPartition> partitionac0 = new HashSet<SystemStreamPartition>();
    partitionac0.add(ac0);
    partitionac0.add(aa0);
    expectedResult.put(new TaskName(ac0.toString()), partitionac0);

    assertEquals(expectedResult, result);
  }
}
