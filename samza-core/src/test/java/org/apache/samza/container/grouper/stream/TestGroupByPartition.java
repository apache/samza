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
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;

public class TestGroupByPartition {
  SystemStreamPartition aa0 = new SystemStreamPartition("SystemA", "StreamA", new Partition(0));
  SystemStreamPartition aa1 = new SystemStreamPartition("SystemA", "StreamA", new Partition(1));
  SystemStreamPartition aa2 = new SystemStreamPartition("SystemA", "StreamA", new Partition(2));
  SystemStreamPartition ab1 = new SystemStreamPartition("SystemA", "StreamB", new Partition(1));
  SystemStreamPartition ab2 = new SystemStreamPartition("SystemA", "StreamB", new Partition(2));
  SystemStreamPartition ac0 = new SystemStreamPartition("SystemA", "StreamB", new Partition(0));

  @Test
  public void testLocalStreamsGroupedCorrectly() {
    HashSet<SystemStreamPartition> allSSPs = new HashSet<SystemStreamPartition>();
    GroupByPartition grouper = new GroupByPartition();
    Map<TaskName, Set<SystemStreamPartition>> emptyResult = grouper.group(allSSPs);
    // empty SSP set gets empty groups
    assertTrue(emptyResult.isEmpty());

    Collections.addAll(allSSPs, aa0, aa1, aa2, ab1, ab2, ac0);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(allSSPs);
    Map<TaskName, Set<SystemStreamPartition>> expectedResult = new HashMap<TaskName, Set<SystemStreamPartition>>();

    HashSet<SystemStreamPartition> partition0 = new HashSet<SystemStreamPartition>();
    partition0.add(aa0);
    partition0.add(ac0);
    expectedResult.put(new TaskName("Partition 0"), partition0);

    HashSet<SystemStreamPartition> partition1 = new HashSet<SystemStreamPartition>();
    partition1.add(aa1);
    partition1.add(ab1);
    expectedResult.put(new TaskName("Partition 1"), partition1);

    HashSet<SystemStreamPartition> partition2 = new HashSet<SystemStreamPartition>();
    partition2.add(aa2);
    partition2.add(ab2);
    expectedResult.put(new TaskName("Partition 2"), partition2);

    assertEquals(expectedResult, result);
  }

  @Test
  public void testBroadcastStreamsGroupedCorrectly() {
    HashMap<String, String> configMap = new HashMap<String, String>();
    configMap.put("task.broadcast.inputs", "SystemA.StreamA#0, SystemA.StreamB#1");
    Config config = new MapConfig(configMap);
    GroupByPartition grouper = new GroupByPartition(config);

    HashSet<SystemStreamPartition> allSSPs = new HashSet<SystemStreamPartition>();
    Collections.addAll(allSSPs, aa0, aa1, aa2, ab1, ab2, ac0);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(allSSPs);

    Map<TaskName, Set<SystemStreamPartition>> expectedResult = new HashMap<TaskName, Set<SystemStreamPartition>>();

    HashSet<SystemStreamPartition> partition0 = new HashSet<SystemStreamPartition>();
    partition0.add(aa0); // broadcast stream
    partition0.add(ac0);
    partition0.add(ab1); // broadcast stream
    expectedResult.put(new TaskName("Partition 0"), partition0);

    HashSet<SystemStreamPartition> partition1 = new HashSet<SystemStreamPartition>();
    partition1.add(aa1);
    partition1.add(ab1); // broadcast stream
    partition1.add(aa0); // broadcast stream
    expectedResult.put(new TaskName("Partition 1"), partition1);

    HashSet<SystemStreamPartition> partition2 = new HashSet<SystemStreamPartition>();
    partition2.add(aa2);
    partition2.add(ab2);
    partition2.add(aa0); // broadcast stream
    partition2.add(ab1); // broadcast stream
    expectedResult.put(new TaskName("Partition 2"), partition2);

    assertEquals(expectedResult, result);
  }

  @Test
  public void testNoTaskOnlyContainsBroadcastStreams() {
    HashMap<String, String> configMap = new HashMap<String, String>();
    configMap.put("task.broadcast.inputs", "SystemA.StreamA#0, SystemA.StreamB#1");
    Config config = new MapConfig(configMap);
    GroupByPartition grouper = new GroupByPartition(config);

    HashSet<SystemStreamPartition> allSSPs = new HashSet<SystemStreamPartition>();
    Collections.addAll(allSSPs, aa0, ab1, ab2);
    Map<TaskName, Set<SystemStreamPartition>> result = grouper.group(allSSPs);

    Map<TaskName, Set<SystemStreamPartition>> expectedResult = new HashMap<TaskName, Set<SystemStreamPartition>>();
    HashSet<SystemStreamPartition> partition2 = new HashSet<SystemStreamPartition>();
    partition2.add(aa0); // broadcast stream
    partition2.add(ab1);
    partition2.add(ab2); // broadcast stream
    expectedResult.put(new TaskName("Partition 2"), partition2);

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

    prevGroupingWithSingleStream.put(new TaskName("Partition 0"), sspSet0);
    prevGroupingWithSingleStream.put(new TaskName("Partition 1"), sspSet1);
    prevGroupingWithSingleStream.put(new TaskName("Partition 2"), sspSet2);
    prevGroupingWithSingleStream.put(new TaskName("Partition 3"), sspSet3);

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
    expectedGrouping.put(new TaskName("Partition 1"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    expectedGrouping.put(new TaskName("Partition 0"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    expectedGrouping.put(new TaskName("Partition 3"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    expectedGrouping.put(new TaskName("Partition 2"), expectedSsps);

    GroupByPartition groupByPartition = new GroupByPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, prevGroupingWithSingleStream);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testMultipleStreamsWithSingleStreamRepartitioning() {
    Map<TaskName, Set<SystemStreamPartition>> prevGroupingWithMultipleStreams = new HashMap<>();

    Set<SystemStreamPartition> sspSet0 = new HashSet<>();
    sspSet0 = new HashSet<>();
    sspSet0.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    sspSet0.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));

    Set<SystemStreamPartition> sspSet1 = new HashSet<>();
    sspSet1 = new HashSet<>();
    sspSet1.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    sspSet1.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));

    Set<SystemStreamPartition> sspSet2 = new HashSet<>();
    sspSet2 = new HashSet<>();
    sspSet2.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    sspSet2.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));

    Set<SystemStreamPartition> sspSet3 = new HashSet<>();
    sspSet3 = new HashSet<>();
    sspSet3.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    sspSet3.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));

    prevGroupingWithMultipleStreams.put(new TaskName("Partition 0"), sspSet0);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 1"), sspSet1);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 2"), sspSet2);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 3"), sspSet3);

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
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    expectedGrouping.put(new TaskName("Partition 1"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    expectedGrouping.put(new TaskName("Partition 0"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    expectedGrouping.put(new TaskName("Partition 3"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    expectedGrouping.put(new TaskName("Partition 2"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    expectedGrouping.put(new TaskName("Partition 5"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    expectedGrouping.put(new TaskName("Partition 4"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));
    expectedGrouping.put(new TaskName("Partition 7"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    expectedGrouping.put(new TaskName("Partition 6"), expectedSsps);

    GroupByPartition groupByPartition = new GroupByPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, prevGroupingWithMultipleStreams);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testOnlyNewlyAddedStreams() {
    Map<TaskName, Set<SystemStreamPartition>> prevGroupingWithMultipleStreams = new HashMap<>();

    Set<SystemStreamPartition> sspSet0 = new HashSet<>();
    sspSet0 = new HashSet<>();
    sspSet0.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    sspSet0.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));

    Set<SystemStreamPartition> sspSet1 = new HashSet<>();
    sspSet1 = new HashSet<>();
    sspSet1.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    sspSet1.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));

    Set<SystemStreamPartition> sspSet2 = new HashSet<>();
    sspSet2 = new HashSet<>();
    sspSet2.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    sspSet2.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));

    Set<SystemStreamPartition> sspSet3 = new HashSet<>();
    sspSet3 = new HashSet<>();
    sspSet3.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    sspSet3.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));

    prevGroupingWithMultipleStreams.put(new TaskName("Partition 0"), sspSet0);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 1"), sspSet1);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 2"), sspSet2);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 3"), sspSet3);

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
    expectedGrouping.put(new TaskName("Partition 1"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    expectedGrouping.put(new TaskName("Partition 0"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    expectedGrouping.put(new TaskName("Partition 3"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    expectedGrouping.put(new TaskName("Partition 2"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    expectedGrouping.put(new TaskName("Partition 5"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    expectedGrouping.put(new TaskName("Partition 4"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));
    expectedGrouping.put(new TaskName("Partition 7"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    expectedGrouping.put(new TaskName("Partition 6"), expectedSsps);

    GroupByPartition groupByPartition = new GroupByPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, prevGroupingWithMultipleStreams);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }


  @Test
  public void testRemovalAndAdditionOfStreamsWithRepartitioning() {
    Map<TaskName, Set<SystemStreamPartition>> prevGroupingWithMultipleStreams = new HashMap<>();

    Set<SystemStreamPartition> sspSet0 = new HashSet<>();
    sspSet0 = new HashSet<>();
    sspSet0.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    sspSet0.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));

    Set<SystemStreamPartition> sspSet1 = new HashSet<>();
    sspSet1 = new HashSet<>();
    sspSet1.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    sspSet1.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));

    Set<SystemStreamPartition> sspSet2 = new HashSet<>();
    sspSet2 = new HashSet<>();
    sspSet2.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    sspSet2.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));

    Set<SystemStreamPartition> sspSet3 = new HashSet<>();
    sspSet3 = new HashSet<>();
    sspSet3.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    sspSet3.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));

    prevGroupingWithMultipleStreams.put(new TaskName("Partition 0"), sspSet0);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 1"), sspSet1);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 2"), sspSet2);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 3"), sspSet3);

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
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    expectedGrouping.put(new TaskName("Partition 1"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    expectedGrouping.put(new TaskName("Partition 0"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    expectedGrouping.put(new TaskName("Partition 3"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    expectedGrouping.put(new TaskName("Partition 2"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    expectedGrouping.put(new TaskName("Partition 5"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    expectedGrouping.put(new TaskName("Partition 4"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));
    expectedGrouping.put(new TaskName("Partition 7"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    expectedGrouping.put(new TaskName("Partition 6"), expectedSsps);


    GroupByPartition groupByPartition = new GroupByPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, prevGroupingWithMultipleStreams);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

  @Test
  public void testMultipleStreamRepartitioningWithNewStreams() {
    Map<TaskName, Set<SystemStreamPartition>> prevGroupingWithMultipleStreams = new HashMap<>();

    Set<SystemStreamPartition> sspSet0 = new HashSet<>();
    sspSet0 = new HashSet<>();
    sspSet0.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    sspSet0.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));

    Set<SystemStreamPartition> sspSet1 = new HashSet<>();
    sspSet1 = new HashSet<>();
    sspSet1.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    sspSet1.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));

    Set<SystemStreamPartition> sspSet2 = new HashSet<>();
    sspSet2 = new HashSet<>();
    sspSet2.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    sspSet2.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));

    Set<SystemStreamPartition> sspSet3 = new HashSet<>();
    sspSet3 = new HashSet<>();
    sspSet3.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    sspSet3.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));

    prevGroupingWithMultipleStreams.put(new TaskName("Partition 0"), sspSet0);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 1"), sspSet1);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 2"), sspSet2);
    prevGroupingWithMultipleStreams.put(new TaskName("Partition 3"), sspSet3);

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
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(5)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(1)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(1)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(5)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(1)));
    expectedGrouping.put(new TaskName("Partition 1"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(0)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(4)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(0)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(4)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(0)));
    expectedGrouping.put(new TaskName("Partition 0"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(7)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(3)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(7)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(3)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(3)));
    expectedGrouping.put(new TaskName("Partition 3"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(2)));
    expectedSsps.add(new SystemStreamPartition("kafka", "PVE", new Partition(6)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(2)));
    expectedSsps.add(new SystemStreamPartition("kafka", "URE", new Partition(6)));
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(2)));
    expectedGrouping.put(new TaskName("Partition 2"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(5)));
    expectedGrouping.put(new TaskName("Partition 5"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(4)));
    expectedGrouping.put(new TaskName("Partition 4"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(7)));
    expectedGrouping.put(new TaskName("Partition 7"), expectedSsps);

    expectedSsps = new HashSet<>();
    expectedSsps.add(new SystemStreamPartition("kafka", "BOB", new Partition(6)));
    expectedGrouping.put(new TaskName("Partition 6"), expectedSsps);

    GroupByPartition groupByPartition = new GroupByPartition();
    Map<TaskName, Set<SystemStreamPartition>> finalGrouping = groupByPartition.group(currSsps, prevGroupingWithMultipleStreams);
    Assert.assertEquals(expectedGrouping, finalGrouping);
  }

}
