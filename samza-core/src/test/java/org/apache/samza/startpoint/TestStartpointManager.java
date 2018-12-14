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
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metadatastore.InMemoryMetadataStore;
import org.apache.samza.metadatastore.InMemoryMetadataStoreFactory;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;


public class TestStartpointManager {

  @Test
  public void testDefaultMetadataStore() {
    MapConfig config = new MapConfig();
    StartpointManager startpointManager = StartpointManager.getWithMetadataStore(new InMemoryMetadataStoreFactory(), config, new NoOpMetricsRegistry());
    Assert.assertNotNull(startpointManager);
    Assert.assertEquals(InMemoryMetadataStore.class, startpointManager.getMetadataStore().getClass());
  }

  @Test
  public void testSpecifiedMetadataStore() {
    MapConfig config = new MapConfig();
    StartpointManager startpointManager = StartpointManager.getWithMetadataStore(new InMemoryMetadataStoreFactory(), config, new NoOpMetricsRegistry());
    Assert.assertNotNull(startpointManager);
    Assert.assertEquals(InMemoryMetadataStore.class, startpointManager.getMetadataStore().getClass());
  }

  @Test
  public void testNoLongerUsableAfterStop() {
    MapConfig config = new MapConfig();
    StartpointManager startpointManager = StartpointManager.getWithMetadataStore(new InMemoryMetadataStoreFactory(), config, new NoOpMetricsRegistry());
    startpointManager.start();
    SystemStreamPartition ssp =
        new SystemStreamPartition("mockSystem", "mockStream", new Partition(2));
    TaskName taskName = new TaskName("MockTask");
    Startpoint startpoint = new StartpointEarliest();

    startpointManager.stop();

    try {
      startpointManager.writeStartpoint(ssp, startpoint);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.writeStartpointForTask(ssp, taskName, startpoint);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.readStartpoint(ssp);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.readStartpointForTask(ssp, taskName);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.deleteStartpoint(ssp);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.deleteStartpointForTask(ssp, taskName);
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.groupStartpointsPerTask(ssp, new JobModel(new MapConfig(), new HashMap<>()));
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }
  }

  @Test
  public void testBasics() {
    MapConfig config = new MapConfig();
    StartpointManager startpointManager = StartpointManager.getWithMetadataStore(new InMemoryMetadataStoreFactory(), config, new NoOpMetricsRegistry());
    startpointManager.start();
    SystemStreamPartition ssp =
        new SystemStreamPartition("mockSystem", "mockStream", new Partition(2));
    TaskName taskName = new TaskName("MockTask");
    StartpointTimestamp startpoint1 = new StartpointTimestamp(111111111L);
    StartpointTimestamp startpoint2 = new StartpointTimestamp(222222222L);
    StartpointSpecific startpoint3 = new StartpointSpecific("1");
    StartpointSpecific startpoint4 = new StartpointSpecific("2");

    // Test createdTimestamp field is null by default
    Assert.assertNotNull(startpoint1.getCreatedTimestamp());
    Assert.assertNotNull(startpoint2.getCreatedTimestamp());
    Assert.assertNotNull(startpoint3.getCreatedTimestamp());
    Assert.assertNotNull(startpoint4.getCreatedTimestamp());

    // Test reads on non-existent keys
    Assert.assertNull(startpointManager.readStartpoint(ssp));
    Assert.assertNull(startpointManager.readStartpointForTask(ssp, taskName));

    // Test writes
    Startpoint startpointFromStore;
    startpointManager.writeStartpoint(ssp, startpoint1);
    startpointManager.writeStartpointForTask(ssp, taskName, startpoint2);
    startpointFromStore = startpointManager.readStartpoint(ssp);
    Assert.assertEquals(StartpointTimestamp.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint1.getTimestampOffset(), ((StartpointTimestamp) startpointFromStore).getTimestampOffset());
    startpointFromStore = startpointManager.readStartpointForTask(ssp, taskName);
    Assert.assertEquals(StartpointTimestamp.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint2.getTimestampOffset(), ((StartpointTimestamp) startpointFromStore).getTimestampOffset());

    // Test overwrites
    startpointManager.writeStartpoint(ssp, startpoint3);
    startpointManager.writeStartpointForTask(ssp, taskName, startpoint4);
    startpointFromStore = startpointManager.readStartpoint(ssp);
    Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint3.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());
    startpointFromStore = startpointManager.readStartpointForTask(ssp, taskName);
    Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint4.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());

    // Test deletes on SSP keys does not affect SSP+TaskName keys
    startpointManager.deleteStartpoint(ssp);
    Assert.assertNull(startpointManager.readStartpoint(ssp));
    Assert.assertNotNull(startpointManager.readStartpointForTask(ssp, taskName));

    // Test deletes on SSP+TaskName keys does not affect SSP keys
    startpointManager.writeStartpoint(ssp, startpoint3);
    startpointManager.deleteStartpointForTask(ssp, taskName);
    Assert.assertNull(startpointManager.readStartpointForTask(ssp, taskName));
    Assert.assertNotNull(startpointManager.readStartpoint(ssp));

    startpointManager.stop();
  }

  @Test
  public void testStaleStartpoints() throws InterruptedException {
    MetadataStore metadataStore = new InMemoryMetadataStoreFactory().getMetadataStore("test-namespace", new MapConfig(),
        new NoOpMetricsRegistry());
    StartpointManager startpointManager = new StartpointManager(metadataStore, new StartpointSerde(), Duration.ofMillis(5));
    SystemStreamPartition ssp =
        new SystemStreamPartition("mockSystem", "mockStream", new Partition(2));
    TaskName taskName = new TaskName("MockTask");

    startpointManager.start();
    startpointManager.writeStartpoint(ssp, new StartpointLatest());
    Thread.sleep(startpointManager.getExpirationDuration().toMillis() + 20);
    Assert.assertNull(startpointManager.readStartpoint(ssp));

    startpointManager.writeStartpointForTask(ssp, taskName, new StartpointLatest());
    Thread.sleep(startpointManager.getExpirationDuration().toMillis() + 2);
    Assert.assertNull(startpointManager.readStartpointForTask(ssp, taskName));
  }

  @Test
  public void testGroupStartpointsPerTask() {
    MapConfig config = new MapConfig();
    StartpointManager startpointManager = StartpointManager.getWithMetadataStore(new InMemoryMetadataStoreFactory(), config, new NoOpMetricsRegistry());
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
    startpointManager.groupStartpointsPerTask(sspBroadcast, jobModel);

    // startpoint42 should remap with key sspSingle to only task "t1"
    startpointManager.groupStartpointsPerTask(sspSingle, jobModel);

    for (TaskName taskName : tasks) {
      // startpoint42 should be mapped to all tasks for sspBroadcast
      Startpoint startpointFromStore = startpointManager.readStartpointForTask(sspBroadcast, taskName);
      Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
      Assert.assertEquals(startpoint42.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());

      // startpoint 42 should be mapped only to task "t1" for sspSingle
      startpointFromStore = startpointManager.readStartpointForTask(sspSingle, taskName);
      if (taskName.getTaskName().equals("t1")) {
        Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
        Assert.assertEquals(startpoint42.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());
      } else {
        Assert.assertNull(startpointFromStore);
      }
    }
    Assert.assertNull(startpointManager.readStartpoint(sspBroadcast));
    Assert.assertNull(startpointManager.readStartpoint(sspSingle));

    // Test startpoints that were explicit assigned to an sspBroadcast2+TaskName will not be overwritten from groupStartpointsPerTask

    StartpointSpecific startpoint1024 = new StartpointSpecific("1024");

    startpointManager.writeStartpoint(sspBroadcast2, startpoint42);
    startpointManager.writeStartpointForTask(sspBroadcast2, tasks.get(1), startpoint1024);
    startpointManager.writeStartpointForTask(sspBroadcast2, tasks.get(3), startpoint1024);

    Set<TaskName> tasksResult = startpointManager.groupStartpointsPerTask(sspBroadcast2, jobModel);
    Assert.assertEquals(tasks.size(), tasksResult.size());
    Assert.assertTrue(tasksResult.containsAll(tasks));

    StartpointSpecific startpointFromStore = (StartpointSpecific) startpointManager.readStartpointForTask(sspBroadcast2, tasks.get(0));
    Assert.assertEquals(startpoint42.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpointForTask(sspBroadcast2, tasks.get(1));
    Assert.assertEquals(startpoint1024.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpointForTask(sspBroadcast2, tasks.get(2));
    Assert.assertEquals(startpoint42.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpointForTask(sspBroadcast2, tasks.get(3));
    Assert.assertEquals(startpoint1024.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpointForTask(sspBroadcast2, tasks.get(4));
    Assert.assertEquals(startpoint42.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    startpointFromStore = (StartpointSpecific) startpointManager.readStartpointForTask(sspBroadcast2, tasks.get(5));
    Assert.assertEquals(startpoint42.getSpecificOffset(), startpointFromStore.getSpecificOffset());
    Assert.assertNull(startpointManager.readStartpoint(sspBroadcast2));

    startpointManager.stop();
  }
}
