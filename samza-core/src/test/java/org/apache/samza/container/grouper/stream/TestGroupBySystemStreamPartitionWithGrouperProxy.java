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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.GrouperMetadata;
import org.apache.samza.container.grouper.task.GrouperMetadataImpl;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;


public class TestGroupBySystemStreamPartitionWithGrouperProxy {

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

    Map<TaskName, Set<SystemStreamPartition>> expectedGroupingForStateful = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1)),
            new SystemStreamPartition("kafka", "PVE", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0)),
            new SystemStreamPartition("kafka", "PVE", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7)),
            new SystemStreamPartition("kafka", "PVE", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2)),
            new SystemStreamPartition("kafka", "PVE", new Partition(6))))
        .build();

    Map<TaskName, Set<SystemStreamPartition>> expectedGroupingForStateless = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(6))))
        .build();

    // SSPGrouperProxy for stateful job
    SSPGrouperProxy groupBySystemStreamPartition = buildSspGrouperProxy(true);
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithSingleStream, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStateful, finalGrouping);

    // SSPGrouperProxy for stateless job
    groupBySystemStreamPartition = buildSspGrouperProxy(false);
    finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStateless, finalGrouping);
  }

  @Test
  public void testMultipleStreamsWithSingleStreamExpansionAndNewStream() {
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

    Map<TaskName, Set<SystemStreamPartition>> expectedGroupingForStateful = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
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

    Map<TaskName, Set<SystemStreamPartition>> expectedGroupingForStateless = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(0))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(6))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(0))))
        .build();

    // SSPGrouperProxy for stateful job
    SSPGrouperProxy groupBySystemStreamPartition = buildSspGrouperProxy(true);
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStateful, finalGrouping);

    // SSPGrouperProxy for stateless job
    groupBySystemStreamPartition = buildSspGrouperProxy(false);
    finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStateless, finalGrouping);
  }

  @Test
  public void testRemovalOfPreviousStreamsAndThenAddNewStream() {
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

    Map<TaskName, Set<SystemStreamPartition>> expectedGroupingForStatefulAndStateless = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(0))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
        .build();

    // SSPGrouperProxy for stateful job
    SSPGrouperProxy groupBySystemStreamPartition = buildSspGrouperProxy(true);
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStatefulAndStateless, finalGrouping);

    // SSPGrouperProxy for stateless job
    groupBySystemStreamPartition = buildSspGrouperProxy(false);
    finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStatefulAndStateless, finalGrouping);
  }

  @Test
  public void testRemovalAndAdditionOfStreamsWithExpansion() {
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

    Map<TaskName, Set<SystemStreamPartition>> expectedGroupingForStateful = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
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

    Map<TaskName, Set<SystemStreamPartition>> expectedGroupingForStateless = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(6))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(0))))
        .build();

    // SSPGrouperProxy for stateful job
    SSPGrouperProxy groupBySystemStreamPartition = buildSspGrouperProxy(true);
    GrouperMetadata grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStateful, finalGrouping);

    // SSPGrouperProxy for stateless job
    groupBySystemStreamPartition = buildSspGrouperProxy(false);
    finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStateless, finalGrouping);
  }

  @Test
  public void testMultipleStreamExpansionWithNewStreams() {
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

    Map<TaskName, Set<SystemStreamPartition>> expectedGroupingForStateful = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
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

    Map<TaskName, Set<SystemStreamPartition>> expectedGroupingForStateless = ImmutableMap.<TaskName, Set<SystemStreamPartition>>builder()
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(0))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(7))))
        .put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "BOB", new Partition(6))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(0))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(6))))
        .put(new TaskName("SystemStreamPartition [kafka, PVE, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "PVE", new Partition(7))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(1))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(2))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(3))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(0))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 4]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(4))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 5]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(5))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 6]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(6))))
        .put(new TaskName("SystemStreamPartition [kafka, URE, 7]"), ImmutableSet.of(new SystemStreamPartition("kafka", "URE", new Partition(7))))
        .build();

    // SSPGrouperProxy for stateful job
    SSPGrouperProxy groupBySystemStreamPartition = buildSspGrouperProxy(true);
    GrouperMetadata
        grouperMetadata = new GrouperMetadataImpl(new HashMap<>(), new HashMap<>(), prevGroupingWithMultipleStreams, new HashMap<>());
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStateful, finalGrouping);

    // SSPGrouperProxy for stateless job
    groupBySystemStreamPartition = buildSspGrouperProxy(false);
    finalGrouping = groupBySystemStreamPartition.group(currSsps, grouperMetadata);
    Assert.assertEquals(expectedGroupingForStateless, finalGrouping);
  }

  private static SSPGrouperProxy buildSspGrouperProxy(boolean forStatefulJob) {
    HashMap<String, String> configMap = new HashMap<>();
    if (forStatefulJob) {
      configMap.put(String.format(StorageConfig.FACTORY, "test-store"), "TestStoreFactory");
    }
    return new SSPGrouperProxy(new MapConfig(configMap), new GroupBySystemStreamPartition(new MapConfig()));
  }
}
