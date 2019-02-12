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

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.GrouperMetadata;
import org.apache.samza.container.grouper.task.GrouperMetadataImpl;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;

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
  public void testSingleStreamRepartitioning() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithSingleStream = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = IntStream.range(0, 8)
            .mapToObj(partitionId -> new SystemStreamPartition("kafka", "PVE", new Partition(partitionId)))
            .collect(Collectors.toSet());

    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(5))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(4))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(6))))
            .build();

    SSPGrouperProxy groupBySystemStreamPartition = new SSPGrouperProxy(new MapConfig(), new GroupBySystemStreamPartition(new MapConfig()));
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithSingleStream, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testMultipleStreamsWithSingleStreamRepartitioning() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithMultipleStreams = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = IntStream.range(0, 8)
            .mapToObj(partitionId -> new SystemStreamPartition("kafka", "PVE", new Partition(partitionId)))
            .collect(Collectors.toSet());
    IntStream.range(0, 8).forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(partitionId))));
    IntStream.range(0, 4).forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(partitionId))));

    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)),
                                                                                        new SystemStreamPartition("kafka", "PVE", new Partition(5))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)),
                                                                                        new SystemStreamPartition("kafka", "PVE", new Partition(6))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(3)),
                                                                                        new SystemStreamPartition("kafka", "PVE", new Partition(7))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)),
                                                                                        new SystemStreamPartition("kafka", "PVE", new Partition(4))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(0))))
            .build();

    SSPGrouperProxy groupBySystemStreamPartition = new SSPGrouperProxy(new MapConfig(), new GroupBySystemStreamPartition(new MapConfig()));
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testOnlyNewlyAddedStreams() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithMultipleStreams = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = IntStream.range(0, 8)
            .mapToObj(partitionId -> new SystemStreamPartition("kafka", "BOB", new Partition(partitionId)))
            .collect(Collectors.toSet());

    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
            .build();

    SSPGrouperProxy groupBySystemStreamPartition = new SSPGrouperProxy(new MapConfig(), new GroupBySystemStreamPartition(new MapConfig()));
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }


  @Test
  public void testRemovalAndAdditionOfStreamsWithRepartitioning() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithMultipleStreams = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = IntStream.range(0, 8)
            .mapToObj(partitionId -> new SystemStreamPartition("kafka", "PVE", new Partition(partitionId)))
            .collect(Collectors.toSet());
    IntStream.range(0, 8).forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(partitionId))));

    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(5)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(6)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(4)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(0))))
            .build();

    SSPGrouperProxy groupBySystemStreamPartition = new SSPGrouperProxy(new MapConfig(), new GroupBySystemStreamPartition(new MapConfig()));
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testMultipleStreamRepartitioningWithNewStreams() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithMultipleStreams = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), ImmutableList.of(new SystemStreamPartition("kafka", "URE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = IntStream.range(0, 8)
                                                   .mapToObj(partitionId -> new SystemStreamPartition("kafka", "PVE", new Partition(partitionId)))
                                                   .collect(Collectors.toSet());
    IntStream.range(0, 8).forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(partitionId))));
    IntStream.range(0, 8).forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(partitionId))));

    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(1))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(0))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(3))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(2))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
            .put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)),
                                                                                        new SystemStreamPartition("kafka", "PVE", new Partition(5))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)),
                                                                                        new SystemStreamPartition("kafka", "PVE", new Partition(6))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(3)),
                                                                                        new SystemStreamPartition("kafka", "PVE", new Partition(7))))
            .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)),
                                                                                        new SystemStreamPartition("kafka", "PVE", new Partition(4))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(1)),
                                                                                        new SystemStreamPartition("kafka", "URE", new Partition(5))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(2)),
                                                                                        new SystemStreamPartition("kafka", "URE", new Partition(6))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(3)),
                                                                                        new SystemStreamPartition("kafka", "URE", new Partition(7))))
            .put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(0)),
                                                                                        new SystemStreamPartition("kafka", "URE", new Partition(4))))
            .build();

    SSPGrouperProxy groupBySystemStreamPartition = new SSPGrouperProxy(new MapConfig(), new GroupBySystemStreamPartition(new MapConfig()));
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }
}
