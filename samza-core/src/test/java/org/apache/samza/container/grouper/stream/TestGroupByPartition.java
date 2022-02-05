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

public class TestGroupByPartition {
  private SystemStreamPartition aa0 = new SystemStreamPartition("SystemA", "StreamA", new Partition(0));
  private SystemStreamPartition aa1 = new SystemStreamPartition("SystemA", "StreamA", new Partition(1));
  private SystemStreamPartition aa2 = new SystemStreamPartition("SystemA", "StreamA", new Partition(2));
  private SystemStreamPartition ab1 = new SystemStreamPartition("SystemA", "StreamB", new Partition(1));
  private SystemStreamPartition ab2 = new SystemStreamPartition("SystemA", "StreamB", new Partition(2));
  private SystemStreamPartition ac0 = new SystemStreamPartition("SystemA", "StreamB", new Partition(0));

  @Test
  public void testLocalStreamsGroupedCorrectly() {
    GroupByPartition grouper = new GroupByPartition(new MapConfig());
    Map<TaskName, Set<SystemStreamPartition>> emptyResult = grouper.group(new HashSet<>());
    assertTrue(emptyResult.isEmpty());

    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(ImmutableSet.of(aa0, aa1, aa2, ab1, ab2, ac0));
    Map<TaskName, Set<SystemStreamPartition>> expectedResult = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 0"), ImmutableSet.of(aa0, ac0))
            .put(new TaskName("Partition 1"), ImmutableSet.of(aa1, ab1))
            .put(new TaskName("Partition 2"), ImmutableSet.of(aa2, ab2))
            .build();

    assertEquals(expectedResult, result);
  }

  @Test
  public void testBroadcastStreamsGroupedCorrectly() {
    Config config = new MapConfig(ImmutableMap.of("task.broadcast.inputs", "SystemA.StreamA#0, SystemA.StreamB#1"));
    GroupByPartition grouper = new GroupByPartition(config);

    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(ImmutableSet.of(aa0, aa1, aa2, ab1, ab2, ac0));

    Map<TaskName, Set<SystemStreamPartition>> expectedResult = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 0"), ImmutableSet.of(aa0, ac0, ab1))
            .put(new TaskName("Partition 1"), ImmutableSet.of(aa1, aa0, ab1))
            .put(new TaskName("Partition 2"), ImmutableSet.of(aa2, aa0, ab2, ab1))
            .build();

    assertEquals(expectedResult, result);
  }

  @Test
  public void testNoTaskOnlyContainsBroadcastStreams() {
    Config config = new MapConfig(ImmutableMap.of("task.broadcast.inputs", "SystemA.StreamA#0, SystemA.StreamB#1"));
    GroupByPartition grouper = new GroupByPartition(config);

    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(ImmutableSet.of(aa0, ab1, ab2));

    Map<TaskName, Set<SystemStreamPartition>> expectedResult = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 2"), ImmutableSet.of(aa0, ab1, ab2)).build();

    assertEquals(expectedResult, result);
  }

  @Test
  public void testElastictyEnabledLocalStreamGroupedCorrectly() {
    Config config = new MapConfig(ImmutableMap.of("job.elasticity.factor", "2"));
    GroupByPartition grouper = new GroupByPartition(config);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(ImmutableSet.of(aa0, aa1, aa2, ab1, ab2, ac0));
    Map<TaskName, Set<SystemStreamPartition>> expectedResult = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
        .put(new TaskName("Partition 0 0"), ImmutableSet.of(new SystemStreamPartition(aa0, 0), new SystemStreamPartition(ac0, 0)))
        .put(new TaskName("Partition 0 1"), ImmutableSet.of(new SystemStreamPartition(aa0, 1), new SystemStreamPartition(ac0, 1)))
        .put(new TaskName("Partition 1 0"), ImmutableSet.of(new SystemStreamPartition(aa1, 0), new SystemStreamPartition(ab1, 0)))
        .put(new TaskName("Partition 1 1"), ImmutableSet.of(new SystemStreamPartition(aa1, 1), new SystemStreamPartition(ab1, 1)))
        .put(new TaskName("Partition 2 0"), ImmutableSet.of(new SystemStreamPartition(aa2, 0), new SystemStreamPartition(ab2, 0)))
        .put(new TaskName("Partition 2 1"), ImmutableSet.of(new SystemStreamPartition(aa2, 1), new SystemStreamPartition(ab2, 1)))
        .build();

    assertEquals(expectedResult, result);
  }
}
