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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class TestGroupBySystemStreamPartition {
  private SystemStreamPartition aa0 = new SystemStreamPartition("SystemA", "StreamA", new Partition(0));
  private SystemStreamPartition aa1 = new SystemStreamPartition("SystemA", "StreamA", new Partition(1));
  private SystemStreamPartition aa2 = new SystemStreamPartition("SystemA", "StreamA", new Partition(2));
  private SystemStreamPartition ac0 = new SystemStreamPartition("SystemA", "StreamB", new Partition(0));
  private GroupBySystemStreamPartitionFactory grouperFactory = new GroupBySystemStreamPartitionFactory();

  @Test
  public void testLocalStreamGroupedCorrectly() {
    SystemStreamPartitionGrouper grouper = grouperFactory.getSystemStreamPartitionGrouper(new MapConfig());
    Map<TaskName, Set<SystemStreamPartition>> emptyResult = grouper.group(new HashSet<>());
    assertTrue(emptyResult.isEmpty());

    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(ImmutableSet.of(aa0, aa1, aa2, ac0));
    Map<TaskName, Set<SystemStreamPartition>> expectedResult = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
                .put(new TaskName(aa0.toString()), ImmutableSet.of(aa0))
                .put(new TaskName(aa1.toString()), ImmutableSet.of(aa1))
                .put(new TaskName(aa2.toString()), ImmutableSet.of(aa2))
                .put(new TaskName(ac0.toString()), ImmutableSet.of(ac0))
                .build();

    assertEquals(expectedResult, result);
  }

  @Test
  public void testBroadcastStreamGroupedCorrectly() {
    Config config = new MapConfig(ImmutableMap.of("task.broadcast.inputs", "SystemA.StreamA#0"));

    SystemStreamPartitionGrouper grouper = new GroupBySystemStreamPartition(config);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(ImmutableSet.of(aa0, aa1, aa2, ac0));

    Map<TaskName, Set<SystemStreamPartition>> expectedResult = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName(aa1.toString()), ImmutableSet.of(aa1, aa0))
            .put(new TaskName(aa2.toString()), ImmutableSet.of(aa2, aa0))
            .put(new TaskName(ac0.toString()), ImmutableSet.of(ac0, aa0))
            .build();

    assertEquals(expectedResult, result);
  }

  @Test
  public void testElasticityEnabledLocalStreamGroupedCorrectly() {
    Config config = new MapConfig(ImmutableMap.of("job.elasticity.factor", "2"));
    SystemStreamPartitionGrouper grouper = grouperFactory.getSystemStreamPartitionGrouper(config);
    Map<TaskName, Set<SystemStreamPartition>> emptyResult = grouper.group(new HashSet<>());
    assertTrue(emptyResult.isEmpty());

    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(ImmutableSet.of(aa0, aa1, aa2, ac0));
    Map<TaskName, Set<SystemStreamPartition>> expectedResult = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
        .put(new TaskName(new SystemStreamPartition(aa0, 0).toString() + "_2"),
            ImmutableSet.of(new SystemStreamPartition(aa0, 0)))
        .put(new TaskName(new SystemStreamPartition(aa0, 1).toString() + "_2"),
            ImmutableSet.of(new SystemStreamPartition(aa0, 1)))
        .put(new TaskName(new SystemStreamPartition(aa1, 0).toString() + "_2"),
            ImmutableSet.of(new SystemStreamPartition(aa1, 0)))
        .put(new TaskName(new SystemStreamPartition(aa1, 1).toString() + "_2"),
            ImmutableSet.of(new SystemStreamPartition(aa1, 1)))
        .put(new TaskName(new SystemStreamPartition(aa2, 0).toString() + "_2"),
            ImmutableSet.of(new SystemStreamPartition(aa2, 0)))
        .put(new TaskName(new SystemStreamPartition(aa2, 1).toString() + "_2"),
            ImmutableSet.of(new SystemStreamPartition(aa2, 1)))
        .put(new TaskName(new SystemStreamPartition(ac0, 0).toString() + "_2"),
            ImmutableSet.of(new SystemStreamPartition(ac0, 0)))
        .put(new TaskName(new SystemStreamPartition(ac0, 1).toString() + "_2"),
            ImmutableSet.of(new SystemStreamPartition(ac0, 1)))
        .build();

    assertEquals(expectedResult, result);
  }
}
