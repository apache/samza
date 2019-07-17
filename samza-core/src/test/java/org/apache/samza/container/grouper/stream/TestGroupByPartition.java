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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

import static org.junit.Assert.*;

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
  public void testSingleStreamRepartitioning() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithSingleStream = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 0"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
            .put(new TaskName("Partition 1"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
            .put(new TaskName("Partition 2"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
            .put(new TaskName("Partition 3"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = IntStream.range(0, 8)
            .mapToObj(partitionId -> new SystemStreamPartition("kafka", "PVE", new Partition(partitionId)))
            .collect(Collectors.toSet());

    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 1"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)),
                                                              new SystemStreamPartition("kafka", "PVE", new Partition(5))))
            .put(new TaskName("Partition 0"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)),
                                                              new SystemStreamPartition("kafka", "PVE", new Partition(4))))
            .put(new TaskName("Partition 3"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7)),
                                                              new SystemStreamPartition("kafka", "PVE", new Partition(3))))
            .put(new TaskName("Partition 2"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)),
                                                              new SystemStreamPartition("kafka", "PVE", new Partition(6))))
            .build();

    SSPGrouperProxy groupByPartition = buildSspGrouperProxy();
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithSingleStream, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testMultipleStreamsWithSingleStreamRepartitioning() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithMultipleStreams = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 0"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)), new SystemStreamPartition("kafka", "URE", new Partition(0))))
            .put(new TaskName("Partition 1"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)), new SystemStreamPartition("kafka", "URE", new Partition(1))))
            .put(new TaskName("Partition 2"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)), new SystemStreamPartition("kafka", "URE", new Partition(2))))
            .put(new TaskName("Partition 3"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3)), new SystemStreamPartition("kafka", "URE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = IntStream.range(0, 8)
            .mapToObj(partitionId -> new SystemStreamPartition("kafka", "PVE", new Partition(partitionId)))
            .collect(Collectors.toSet());
    IntStream.range(0, 4)
             .forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(partitionId))));
    IntStream.range(0, 8)
             .forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(partitionId))));


    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 1"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)),
                                                              new SystemStreamPartition("kafka", "PVE", new Partition(5)),
                                                              new SystemStreamPartition("kafka", "URE", new Partition(1)),
                                                              new SystemStreamPartition("kafka", "BOB", new Partition(1))))
            .put(new TaskName("Partition 0"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)),
                                                              new SystemStreamPartition("kafka", "PVE", new Partition(4)),
                                                              new SystemStreamPartition("kafka", "URE", new Partition(0)),
                                                              new SystemStreamPartition("kafka", "BOB", new Partition(0))))
            .put(new TaskName("Partition 3"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7)),
                                                              new SystemStreamPartition("kafka", "PVE", new Partition(3)),
                                                              new SystemStreamPartition("kafka", "URE", new Partition(3)),
                                                              new SystemStreamPartition("kafka", "BOB", new Partition(3))))
            .put(new TaskName("Partition 2"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)),
                                                              new SystemStreamPartition("kafka", "PVE", new Partition(6)),
                                                              new SystemStreamPartition("kafka", "URE", new Partition(2)),
                                                              new SystemStreamPartition("kafka", "BOB", new Partition(2))))
            .put(new TaskName("Partition 5"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
            .put(new TaskName("Partition 4"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
            .put(new TaskName("Partition 7"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
            .put(new TaskName("Partition 6"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
            .build();

    SSPGrouperProxy groupByPartition = buildSspGrouperProxy();
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testOnlyNewlyAddedStreams() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithMultipleStreams = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 0"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)), new SystemStreamPartition("kafka", "URE", new Partition(0))))
            .put(new TaskName("Partition 1"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)), new SystemStreamPartition("kafka", "URE", new Partition(1))))
            .put(new TaskName("Partition 2"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)), new SystemStreamPartition("kafka", "URE", new Partition(2))))
            .put(new TaskName("Partition 3"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3)), new SystemStreamPartition("kafka", "URE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = IntStream.range(0, 8)
            .mapToObj(partitionId -> new SystemStreamPartition("kafka", "BOB", new Partition(partitionId)))
            .collect(Collectors.toSet());

    // expected Grouping
    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 5"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
            .put(new TaskName("Partition 4"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
            .put(new TaskName("Partition 7"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
            .put(new TaskName("Partition 6"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
            .put(new TaskName("Partition 0"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(0))))
            .put(new TaskName("Partition 1"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(1))))
            .put(new TaskName("Partition 2"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(2))))
            .put(new TaskName("Partition 3"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(3))))
            .build();

    SSPGrouperProxy groupByPartition = buildSspGrouperProxy();
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }


  @Test
  public void testRemovalAndAdditionOfStreamsWithRepartitioning() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithMultipleStreams = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 0"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)), new SystemStreamPartition("kafka", "URE", new Partition(0))))
            .put(new TaskName("Partition 1"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)), new SystemStreamPartition("kafka", "URE", new Partition(1))))
            .put(new TaskName("Partition 2"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)), new SystemStreamPartition("kafka", "URE", new Partition(2))))
            .put(new TaskName("Partition 3"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3)), new SystemStreamPartition("kafka", "URE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = IntStream.range(0, 8)
            .mapToObj(partitionId -> new SystemStreamPartition("kafka", "BOB", new Partition(partitionId)))
            .collect(Collectors.toSet());

    IntStream.range(0, 8)
             .forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(partitionId))));

    // expected grouping
    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 1"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(5)),
                    new SystemStreamPartition("kafka", "BOB", new Partition(1))))
            .put(new TaskName("Partition 0"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(4)),
                    new SystemStreamPartition("kafka", "BOB", new Partition(0))))
            .put(new TaskName("Partition 3"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(3)),
                    new SystemStreamPartition("kafka", "BOB", new Partition(3))))
            .put(new TaskName("Partition 2"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(6)),
                    new SystemStreamPartition("kafka", "BOB", new Partition(2))))
            .put(new TaskName("Partition 5"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
            .put(new TaskName("Partition 4"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
            .put(new TaskName("Partition 7"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
            .put(new TaskName("Partition 6"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
            .build();

    SSPGrouperProxy groupByPartition = buildSspGrouperProxy();
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testMultipleStreamRepartitioningWithNewStreams() {
    Map<TaskName, List<SystemStreamPartition>> prevGroupingWithMultipleStreams = ImmutableMap.<TaskName, List<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 0"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)), new SystemStreamPartition("kafka", "URE", new Partition(0))))
            .put(new TaskName("Partition 1"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)), new SystemStreamPartition("kafka", "URE", new Partition(1))))
            .put(new TaskName("Partition 2"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)), new SystemStreamPartition("kafka", "URE", new Partition(2))))
            .put(new TaskName("Partition 3"), ImmutableList.of(new SystemStreamPartition("kafka", "PVE", new Partition(3)), new SystemStreamPartition("kafka", "URE", new Partition(3))))
            .build();

    Set<SystemStreamPartition> currSsps = new HashSet<>();
    IntStream.range(0, 8)
             .forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(partitionId))));
    IntStream.range(0, 8)
             .forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(partitionId))));
    IntStream.range(0, 8)
             .forEach(partitionId -> currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(partitionId))));

    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
            .put(new TaskName("Partition 1"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(5)),
                    new SystemStreamPartition("kafka", "URE", new Partition(1)),
                    new SystemStreamPartition("kafka", "URE", new Partition(5)),
                    new SystemStreamPartition("kafka", "BOB", new Partition(1))))
            .put(new TaskName("Partition 0"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(4)),
                    new SystemStreamPartition("kafka", "URE", new Partition(0)),
                    new SystemStreamPartition("kafka", "URE", new Partition(4)),
                    new SystemStreamPartition("kafka", "BOB", new Partition(0))))
            .put(new TaskName("Partition 3"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(3)),
                    new SystemStreamPartition("kafka", "URE", new Partition(3)),
                    new SystemStreamPartition("kafka", "URE", new Partition(7)),
                    new SystemStreamPartition("kafka", "BOB", new Partition(3))))
            .put(new TaskName("Partition 2"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)),
                    new SystemStreamPartition("kafka", "PVE", new Partition(6)),
                    new SystemStreamPartition("kafka", "URE", new Partition(2)),
                    new SystemStreamPartition("kafka", "URE", new Partition(6)),
                    new SystemStreamPartition("kafka", "BOB", new Partition(2))))
            .put(new TaskName("Partition 5"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
            .put(new TaskName("Partition 4"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
            .put(new TaskName("Partition 7"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
            .put(new TaskName("Partition 6"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
            .build();


    SSPGrouperProxy groupByPartition = buildSspGrouperProxy();
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  private SSPGrouperProxy buildSspGrouperProxy() {
    return new SSPGrouperProxy(new MapConfig(), new GroupByPartition(new MapConfig()), getClass().getClassLoader());
  }
}
