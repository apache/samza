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

package org.apache.samza.storage;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContextImpl;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;


public class TestKafkaChangelogStateBackendFactory {

  @Test
  public void testGetChangelogSSP() {
    KafkaChangelogStateBackendFactory factory = new KafkaChangelogStateBackendFactory();
    TaskName taskName0 = new TaskName("task0");
    TaskName taskName1 = new TaskName("task1");
    TaskModel taskModel0 = new TaskModel(taskName0,
        ImmutableSet.of(new SystemStreamPartition("input", "stream", new Partition(0))),
        new Partition(10));
    TaskModel taskModel1 = new TaskModel(taskName1,
        ImmutableSet.of(new SystemStreamPartition("input", "stream", new Partition(1))), new Partition(11));
    ContainerModel containerModel = new ContainerModel("processorId",
        ImmutableMap.of(taskName0, taskModel0, taskName1, taskModel1));
    Map<String, SystemStream> changeLogSystemStreams = ImmutableMap.of(
        "store0", new SystemStream("changelogSystem0", "store0-changelog"),
        "store1", new SystemStream("changelogSystem1", "store1-changelog"));
    Set<SystemStreamPartition> expected = ImmutableSet.of(
        new SystemStreamPartition("changelogSystem0", "store0-changelog", new Partition(10)),
        new SystemStreamPartition("changelogSystem1", "store1-changelog", new Partition(10)),
        new SystemStreamPartition("changelogSystem0", "store0-changelog", new Partition(11)),
        new SystemStreamPartition("changelogSystem1", "store1-changelog", new Partition(11)));
    Assert.assertEquals(expected, factory.getChangelogSSPForContainer(changeLogSystemStreams,
        new ContainerContextImpl(containerModel, null, null)));
  }

  @Test
  public void testGetChangelogSSPsForContainerNoChangelogs() {
    KafkaChangelogStateBackendFactory factory = new KafkaChangelogStateBackendFactory();
    TaskName taskName0 = new TaskName("task0");
    TaskName taskName1 = new TaskName("task1");
    TaskModel taskModel0 = new TaskModel(taskName0,
        ImmutableSet.of(new SystemStreamPartition("input", "stream", new Partition(0))),
        new Partition(10));
    TaskModel taskModel1 = new TaskModel(taskName1,
        ImmutableSet.of(new SystemStreamPartition("input", "stream", new Partition(1))),
        new Partition(11));
    ContainerModel containerModel = new ContainerModel("processorId",
        ImmutableMap.of(taskName0, taskModel0, taskName1, taskModel1));
    Assert.assertEquals(Collections.emptySet(), factory.getChangelogSSPForContainer(Collections.emptyMap(),
        new ContainerContextImpl(containerModel, null, null)));
  }
}
