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
package org.apache.samza.startpoint;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStoreTestUtil;
import org.apache.samza.coordinator.metadatastore.NamespaceAwareCoordinatorStreamStore;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestStartpointManager {

  private static final Config CONFIG = new MapConfig(ImmutableMap.of("job.name", "test-job", "job.coordinator.system", "test-kafka"));

  private CoordinatorStreamStore coordinatorStreamStore;
  private StartpointManager startpointManager;

  @Before
  public void setup() {
    CoordinatorStreamStoreTestUtil coordinatorStreamStoreTestUtil = new CoordinatorStreamStoreTestUtil(CONFIG);
    coordinatorStreamStore = coordinatorStreamStoreTestUtil.getCoordinatorStreamStore();
    startpointManager = new StartpointManager(coordinatorStreamStore);
  }

  @Test
  public void testDefaultMetadataStore() {
    StartpointManager startpointManager = new StartpointManager(coordinatorStreamStore);
    Assert.assertNotNull(startpointManager);
    Assert.assertEquals(NamespaceAwareCoordinatorStreamStore.class, startpointManager.getMetadataStore().getClass());
  }

  @Test
  public void testStaleStartpoints() {
    SystemStreamPartition ssp = new SystemStreamPartition("mockSystem", "mockStream", new Partition(2));
    TaskName taskName = new TaskName("MockTask");

    startpointManager.start();
    long staleTimestamp = Instant.now().toEpochMilli() - StartpointManager.DEFAULT_EXPIRATION_DURATION.toMillis() - 2;
    StartpointTimestamp startpoint = new StartpointTimestamp(staleTimestamp);
    startpointManager.writeStartpoint(ssp, startpoint);
    Assert.assertNull(startpointManager.readStartpoint(ssp));

    startpointManager.writeStartpoint(ssp, taskName, startpoint);
    Assert.assertNull(startpointManager.readStartpoint(ssp, taskName));
  }

  @Test
  public void testNoLongerUsableAfterStop() {
    StartpointManager startpointManager = new StartpointManager(coordinatorStreamStore);
    startpointManager.start();
    SystemStreamPartition ssp =
        new SystemStreamPartition("mockSystem", "mockStream", new Partition(2));
    TaskName taskName = new TaskName("MockTask");
    Startpoint startpoint = new StartpointOldest();

    startpointManager.stop();

    try {
      startpointManager.writeStartpoint(ssp, startpoint);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.writeStartpoint(ssp, taskName, startpoint);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.readStartpoint(ssp);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.readStartpoint(ssp, taskName);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.deleteStartpoint(ssp);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.deleteStartpoint(ssp, taskName);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.fanOutStartpointsToTasks(new JobModel(new MapConfig(), new HashMap<>()));
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }
  }

  @Test
  public void testBasics() {
    startpointManager.start();
    SystemStreamPartition ssp =
        new SystemStreamPartition("mockSystem", "mockStream", new Partition(2));
    TaskName taskName = new TaskName("MockTask");
    StartpointTimestamp startpoint1 = new StartpointTimestamp(111111111L);
    StartpointTimestamp startpoint2 = new StartpointTimestamp(222222222L);
    StartpointSpecific startpoint3 = new StartpointSpecific("1");
    StartpointSpecific startpoint4 = new StartpointSpecific("2");

    // Test createdTimestamp field is not null by default
    Assert.assertNotNull(startpoint1.getCreationTimestamp());
    Assert.assertNotNull(startpoint2.getCreationTimestamp());
    Assert.assertNotNull(startpoint3.getCreationTimestamp());
    Assert.assertNotNull(startpoint4.getCreationTimestamp());

    // Test reads on non-existent keys
    Assert.assertNull(startpointManager.readStartpoint(ssp));
    Assert.assertNull(startpointManager.readStartpoint(ssp, taskName));

    // Test writes
    Startpoint startpointFromStore;
    startpointManager.writeStartpoint(ssp, startpoint1);
    startpointManager.writeStartpoint(ssp, taskName, startpoint2);
    startpointFromStore = startpointManager.readStartpoint(ssp);
    Assert.assertEquals(StartpointTimestamp.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint1.getTimestampOffset(), ((StartpointTimestamp) startpointFromStore).getTimestampOffset());
    Assert.assertTrue(startpointFromStore.getCreationTimestamp() <= Instant.now().toEpochMilli());
    startpointFromStore = startpointManager.readStartpoint(ssp, taskName);
    Assert.assertEquals(StartpointTimestamp.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint2.getTimestampOffset(), ((StartpointTimestamp) startpointFromStore).getTimestampOffset());
    Assert.assertTrue(startpointFromStore.getCreationTimestamp() <= Instant.now().toEpochMilli());

    // Test overwrites
    startpointManager.writeStartpoint(ssp, startpoint3);
    startpointManager.writeStartpoint(ssp, taskName, startpoint4);
    startpointFromStore = startpointManager.readStartpoint(ssp);
    Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint3.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());
    Assert.assertTrue(startpointFromStore.getCreationTimestamp() <= Instant.now().toEpochMilli());
    startpointFromStore = startpointManager.readStartpoint(ssp, taskName);
    Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint4.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());
    Assert.assertTrue(startpointFromStore.getCreationTimestamp() <= Instant.now().toEpochMilli());

    // Test deletes on SSP keys does not affect SSP+TaskName keys
    startpointManager.deleteStartpoint(ssp);
    Assert.assertNull(startpointManager.readStartpoint(ssp));
    Assert.assertNotNull(startpointManager.readStartpoint(ssp, taskName));

    // Test deletes on SSP+TaskName keys does not affect SSP keys
    startpointManager.writeStartpoint(ssp, startpoint3);
    startpointManager.deleteStartpoint(ssp, taskName);
    Assert.assertNull(startpointManager.readStartpoint(ssp, taskName));
    Assert.assertNotNull(startpointManager.readStartpoint(ssp));

    startpointManager.stop();
  }

  @Test
  public void testGroupStartpointsPerTask() {
    MapConfig config = new MapConfig();
    startpointManager.start();
    SystemStreamPartition sspBroadcast =
        new SystemStreamPartition("mockSystem1", "mockStream1", new Partition(2));
    SystemStreamPartition sspBroadcast2 =
        new SystemStreamPartition("mockSystem3", "mockStream3", new Partition(4));
    SystemStreamPartition sspSingle =
        new SystemStreamPartition("mockSystem2", "mockStream2", new Partition(3));

    List<TaskName> tasks =
        ImmutableList.of(new TaskName("t0"), new TaskName("t1"), new TaskName("t2"), new TaskName("t3"), new TaskName("t4"), new TaskName("t5"));

    Map<TaskName, TaskModel> taskModelMap = tasks.stream()
        .map(task -> new TaskModel(task, task.getTaskName().equals("t1") ? ImmutableSet.of(sspBroadcast, sspBroadcast2, sspSingle) : ImmutableSet.of(sspBroadcast, sspBroadcast2), new Partition(1)))
        .collect(Collectors.toMap(taskModel -> taskModel.getTaskName(), taskModel -> taskModel));
    ContainerModel containerModel = new ContainerModel("container 0", taskModelMap);
    JobModel jobModel = new JobModel(config, ImmutableMap.of(containerModel.getId(), containerModel));

    StartpointSpecific startpoint42 = new StartpointSpecific("42");

    startpointManager.writeStartpoint(sspBroadcast, startpoint42);
    startpointManager.writeStartpoint(sspSingle, startpoint42);

    // startpoint42 should remap with key sspBroadcast to all tasks + sspBroadcast
    Set<SystemStreamPartition> systemStreamPartitions = startpointManager.fanOutStartpointsToTasks(jobModel);
    Assert.assertEquals(2, systemStreamPartitions.size());
    Assert.assertTrue(systemStreamPartitions.containsAll(ImmutableSet.of(sspBroadcast, sspSingle)));

    for (TaskName taskName : tasks) {
      // startpoint42 should be mapped to all tasks for sspBroadcast
      Startpoint startpointFromStore = startpointManager.readStartpoint(sspBroadcast, taskName);
      Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
      Assert.assertEquals(startpoint42.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());

      // startpoint 42 should be mapped only to task "t1" for sspSingle
      startpointFromStore = startpointManager.readStartpoint(sspSingle, taskName);
      if (taskName.getTaskName().equals("t1")) {
        Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
        Assert.assertEquals(startpoint42.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());
      } else {
        Assert.assertNull(startpointFromStore);
      }
    }
    Assert.assertNull(startpointManager.readStartpoint(sspBroadcast));
    Assert.assertNull(startpointManager.readStartpoint(sspSingle));

    // Test startpoints that were explicit assigned to an sspBroadcast2+TaskName will not be overwritten from fanOutStartpointsToTasks

    StartpointSpecific startpoint1024 = new StartpointSpecific("1024");

    startpointManager.writeStartpoint(sspBroadcast2, startpoint42);
    startpointManager.writeStartpoint(sspBroadcast2, tasks.get(1), startpoint1024);
    startpointManager.writeStartpoint(sspBroadcast2, tasks.get(3), startpoint1024);

    Set<SystemStreamPartition> sspsDeleted = startpointManager.fanOutStartpointsToTasks(jobModel);
    Assert.assertEquals(1, sspsDeleted.size());
    Assert.assertTrue(sspsDeleted.contains(sspBroadcast2));

    StartpointSpecific startpointFromStore = (StartpointSpecific) startpointManager.readStartpoint(sspBroadcast2, tasks.get(0));
    Assert.assertEquals(startpoint42.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpoint(sspBroadcast2, tasks.get(1));
    Assert.assertEquals(startpoint1024.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpoint(sspBroadcast2, tasks.get(2));
    Assert.assertEquals(startpoint42.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpoint(sspBroadcast2, tasks.get(3));
    Assert.assertEquals(startpoint1024.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpoint(sspBroadcast2, tasks.get(4));
    Assert.assertEquals(startpoint42.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpoint(sspBroadcast2, tasks.get(5));
    Assert.assertEquals(startpoint42.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    Assert.assertNull(startpointManager.readStartpoint(sspBroadcast2));

    startpointManager.stop();
  }
}
