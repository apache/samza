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
import org.junit.Assert;
import org.junit.Test;

public class TestGroupBySystemStreamPartition {
  SystemStreamPartition aa0 = new SystemStreamPartition("SystemA", "StreamA", new Partition(0));
  SystemStreamPartition aa1 = new SystemStreamPartition("SystemA", "StreamA", new Partition(1));
  SystemStreamPartition aa2 = new SystemStreamPartition("SystemA", "StreamA", new Partition(2));
  SystemStreamPartition ac0 = new SystemStreamPartition("SystemA", "StreamB", new Partition(0));
  GroupBySystemStreamPartitionFactory grouperFactory = new GroupBySystemStreamPartitionFactory();

  @Test
  public void testLocalStreamGroupedCorrectly() {
    HashSet<SystemStreamPartition> allSSPs = new HashSet<SystemStreamPartition>();
    HashMap<String, String> configMap = new HashMap<String, String>();
    Config config = new MapConfig(configMap);

    SystemStreamPartitionGrouper grouper = grouperFactory.getSystemStreamPartitionGrouper(config);
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
    GroupBySystemStreamPartitionFactory grouperFactory = new GroupBySystemStreamPartitionFactory();
    SystemStreamPartitionGrouper grouper = grouperFactory.getSystemStreamPartitionGrouper(config);
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


  @Test
  public void testSingleStreamRepartitioning() {
    Map<TaskName, Set<SystemStreamPartition>> prevGroupingWithSingleStream = new HashMap<>();
    Set<SystemStreamPartition> sspSet0 = new HashSet<>();
    sspSet0.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));

    Set<SystemStreamPartition> sspSet1 = new HashSet<>();
    sspSet1.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));

    Set<SystemStreamPartition> sspSet2 = new HashSet<>();
    sspSet2.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));

    Set<SystemStreamPartition> sspSet3 = new HashSet<>();
    sspSet3.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));

    prevGroupingWithSingleStream.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), sspSet0);
    prevGroupingWithSingleStream.put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), sspSet1);
    prevGroupingWithSingleStream.put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), sspSet2);
    prevGroupingWithSingleStream.put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), sspSet3);

    Set<SystemStreamPartition> currSsps = new HashSet<>();
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));

    // expected grouping
    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = new HashMap<>();
    Set<SystemStreamPartition> expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), expectedSsps);

    GroupBySystemStreamPartition groupBySystemStreamPartition = new GroupBySystemStreamPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, prevGroupingWithSingleStream);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testMultipleStreamsWithSingleStreamRepartitioning() {
    Map<TaskName, Set<SystemStreamPartition>> prevGroupingWithMultipleStreams = new HashMap<>();

    Set<SystemStreamPartition> sspSet0 = new HashSet<>();
    sspSet0.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));

    Set<SystemStreamPartition> sspSet1 = new HashSet<>();
    sspSet1.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));

    Set<SystemStreamPartition> sspSet2 = new HashSet<>();
    sspSet2.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));

    Set<SystemStreamPartition> sspSet3 = new HashSet<>();
    sspSet3.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));

    Set<SystemStreamPartition> sspSet4 = new HashSet<>();
    sspSet4.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));

    Set<SystemStreamPartition> sspSet5 = new HashSet<>();
    sspSet5.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));

    Set<SystemStreamPartition> sspSet6 = new HashSet<>();
    sspSet6.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));

    Set<SystemStreamPartition> sspSet7 = new HashSet<>();
    sspSet7.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));

    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), sspSet0);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), sspSet1);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), sspSet2);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), sspSet3);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), sspSet4);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), sspSet5);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), sspSet6);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), sspSet7);

    Set<SystemStreamPartition> currSsps = new HashSet<>();
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));

    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));

    // newly added Streams
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));

    // expected final grouping
    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = new HashMap<>();
    Set<SystemStreamPartition> expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), expectedSsps);

    GroupBySystemStreamPartition groupBySystemStreamPartition = new GroupBySystemStreamPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, prevGroupingWithMultipleStreams);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testOnlyNewlyAddedStreams() {
    Map<TaskName, Set<SystemStreamPartition>> prevGroupingWithMultipleStreams = new HashMap<>();

    Set<SystemStreamPartition> sspSet0 = new HashSet<>();
    sspSet0.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));

    Set<SystemStreamPartition> sspSet1 = new HashSet<>();
    sspSet1.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));

    Set<SystemStreamPartition> sspSet2 = new HashSet<>();
    sspSet2.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));

    Set<SystemStreamPartition> sspSet3 = new HashSet<>();
    sspSet3.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));

    Set<SystemStreamPartition> sspSet4 = new HashSet<>();
    sspSet4.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));

    Set<SystemStreamPartition> sspSet5 = new HashSet<>();
    sspSet5.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));

    Set<SystemStreamPartition> sspSet6 = new HashSet<>();
    sspSet6.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));

    Set<SystemStreamPartition> sspSet7 = new HashSet<>();
    sspSet7.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));

    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), sspSet0);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), sspSet1);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), sspSet2);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), sspSet3);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), sspSet4);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), sspSet5);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), sspSet6);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), sspSet7);

    Set<SystemStreamPartition> currSsps = new HashSet<>();
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));

    // expected Grouping
    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = new HashMap<>();
    Set<SystemStreamPartition> expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), expectedSsps);

    GroupBySystemStreamPartition groupBySystemStreamPartition = new GroupBySystemStreamPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, prevGroupingWithMultipleStreams);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }


  @Test
  public void testRemovalAndAdditionOfStreamsWithRepartitioning() {
    Map<TaskName, Set<SystemStreamPartition>> prevGroupingWithMultipleStreams = new HashMap<>();

    Set<SystemStreamPartition> sspSet0 = new HashSet<>();
    sspSet0.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));

    Set<SystemStreamPartition> sspSet1 = new HashSet<>();
    sspSet1.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));

    Set<SystemStreamPartition> sspSet2 = new HashSet<>();
    sspSet2.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));

    Set<SystemStreamPartition> sspSet3 = new HashSet<>();
    sspSet3.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));

    Set<SystemStreamPartition> sspSet4 = new HashSet<>();
    sspSet4.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));

    Set<SystemStreamPartition> sspSet5 = new HashSet<>();
    sspSet5.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));

    Set<SystemStreamPartition> sspSet6 = new HashSet<>();
    sspSet6.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));

    Set<SystemStreamPartition> sspSet7 = new HashSet<>();
    sspSet7.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));

    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), sspSet0);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), sspSet1);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), sspSet2);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), sspSet3);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), sspSet4);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), sspSet5);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), sspSet6);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), sspSet7);

    Set<SystemStreamPartition> currSsps = new HashSet<>();
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));

    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));

    // expected grouping
    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = new HashMap<>();
    Set<SystemStreamPartition> expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), expectedSsps);

    GroupBySystemStreamPartition groupBySystemStreamPartition = new GroupBySystemStreamPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, prevGroupingWithMultipleStreams);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testMultipleStreamRepartitioningWithNewStreams() {
    Map<TaskName, Set<SystemStreamPartition>> prevGroupingWithMultipleStreams = new HashMap<>();

    Set<SystemStreamPartition> sspSet0 = new HashSet<>();
    sspSet0.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));

    Set<SystemStreamPartition> sspSet1 = new HashSet<>();
    sspSet1.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));

    Set<SystemStreamPartition> sspSet2 = new HashSet<>();
    sspSet2.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));

    Set<SystemStreamPartition> sspSet3 = new HashSet<>();
    sspSet3.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));

    Set<SystemStreamPartition> sspSet4 = new HashSet<>();
    sspSet4.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));

    Set<SystemStreamPartition> sspSet5 = new HashSet<>();
    sspSet5.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));

    Set<SystemStreamPartition> sspSet6 = new HashSet<>();
    sspSet6.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));

    Set<SystemStreamPartition> sspSet7 = new HashSet<>();
    sspSet7.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));

    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), sspSet0);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), sspSet1);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), sspSet2);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), sspSet3);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), sspSet4);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), sspSet5);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), sspSet6);
    prevGroupingWithMultipleStreams.put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), sspSet7);

    Set<SystemStreamPartition> currSsps = new HashSet<>();
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    currSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(4)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(5)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(6)));
    currSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(7)));

    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    currSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));

    // expected grouping
    Map<TaskName, Set<SystemStreamPartition>> expectedGrouping = new HashMap<>();
    Set<SystemStreamPartition> expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 2]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 0]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 7]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 6]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 5]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 4]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 2]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 3]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(4)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, URE, 0]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(5)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, URE, 1]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(6)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, URE, 2]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(7)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, URE, 3]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, PVE, 0]"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    expectedGrouping.put(new TaskName("SystemStreamPartition [kafka, BOB, 3]"), expectedSsps);

    GroupBySystemStreamPartition groupBySystemStreamPartition = new GroupBySystemStreamPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupBySystemStreamPartition.group(currSsps, prevGroupingWithMultipleStreams);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }
}
