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

package org.apache.samza.config;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Test;

public class TestTaskConfigJava {

  @Test
  public void testGetBroadcastSystemStreamPartitions() {
    HashMap<String, String> map = new HashMap<String, String>();
    map.put("task.broadcast.inputs", "kafka.foo#4, kafka.boo#5, kafka.z-o-o#[12-14], kafka.foo.bar#[3-4]");
    Config config = new MapConfig(map);

    TaskConfigJava taskConfig = new TaskConfigJava(config);
    Set<SystemStreamPartition> systemStreamPartitionSet = taskConfig.getBroadcastSystemStreamPartitions();

    HashSet<SystemStreamPartition> expected = new HashSet<SystemStreamPartition>();
    expected.add(new SystemStreamPartition("kafka", "foo", new Partition(4)));
    expected.add(new SystemStreamPartition("kafka", "boo", new Partition(5)));
    expected.add(new SystemStreamPartition("kafka", "z-o-o", new Partition(12)));
    expected.add(new SystemStreamPartition("kafka", "z-o-o", new Partition(13)));
    expected.add(new SystemStreamPartition("kafka", "z-o-o", new Partition(14)));
    expected.add(new SystemStreamPartition("kafka", "foo.bar", new Partition(3)));
    expected.add(new SystemStreamPartition("kafka", "foo.bar", new Partition(4)));
    assertEquals(expected, systemStreamPartitionSet);

    map.put("task.broadcast.inputs", "kafka.foo");
    taskConfig = new TaskConfigJava(new MapConfig(map));
    boolean catchCorrectException = false;
    try {
      taskConfig.getBroadcastSystemStreamPartitions();
    } catch (IllegalArgumentException e) {
      catchCorrectException = true;
    }
    assertTrue(catchCorrectException);

    map.put("task.broadcast.inputs", "kafka.org.apache.events.WhitelistedIps#1-2");
    taskConfig = new TaskConfigJava(new MapConfig(map));
    boolean invalidFormatException = false;
    try {
      taskConfig.getBroadcastSystemStreamPartitions();
    } catch (IllegalArgumentException e) {
      invalidFormatException = true;
    }
    assertTrue(invalidFormatException);
  }
}
