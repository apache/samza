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
import java.io.IOException;
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
import org.apache.samza.system.SystemStreamPartition;
import org.junit.After;
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
    coordinatorStreamStore.init();
    startpointManager = new StartpointManager(coordinatorStreamStore);
    startpointManager.start();
  }

  @After
  public void teardown() {
    startpointManager.stop();
    coordinatorStreamStore.close();
  }

  @Test
  public void testDefaultMetadataStore() {
    StartpointManager startpointManager = new StartpointManager(coordinatorStreamStore);
    Assert.assertNotNull(startpointManager);
    Assert.assertEquals(NamespaceAwareCoordinatorStreamStore.class, startpointManager.getReadWriteStore().getClass());
    Assert.assertEquals(NamespaceAwareCoordinatorStreamStore.class, startpointManager.getFanOutStore().getClass());
  }

  @Test
  public void testStaleStartpoints() {
    SystemStreamPartition ssp = new SystemStreamPartition("mockSystem", "mockStream", new Partition(2));
    TaskName taskName = new TaskName("MockTask");

    long staleTimestamp = Instant.now().toEpochMilli() - StartpointManager.DEFAULT_EXPIRATION_DURATION.toMillis() - 2;
    StartpointTimestamp startpoint = new StartpointTimestamp(staleTimestamp, staleTimestamp);

    startpointManager.writeStartpoint(ssp, startpoint);
    Assert.assertFalse(startpointManager.readStartpoint(ssp).isPresent());

    startpointManager.writeStartpoint(ssp, taskName, startpoint);
    Assert.assertFalse(startpointManager.readStartpoint(ssp, taskName).isPresent());
  }

  @Test
  public void testNoLongerUsableAfterStop() throws IOException {
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
      startpointManager.fanOut(new HashMap<>());
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.getFanOutForTask(new TaskName("t0"));
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.removeFanOutForTask(new TaskName("t0"));
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }

    try {
      startpointManager.removeFanOutForTaskSSPs(new TaskName("t0"), ImmutableSet.of(ssp));
      Assert.fail("Expected precondition exception.");
    } catch (IllegalStateException ex) { }
  }

  @Test
  public void testBasics() {
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
    Assert.assertFalse(startpointManager.readStartpoint(ssp).isPresent());
    Assert.assertFalse(startpointManager.readStartpoint(ssp, taskName).isPresent());

    // Test writes
    Startpoint startpointFromStore;
    startpointManager.writeStartpoint(ssp, startpoint1);
    startpointManager.writeStartpoint(ssp, taskName, startpoint2);
    startpointFromStore = startpointManager.readStartpoint(ssp).get();
    Assert.assertEquals(StartpointTimestamp.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint1.getTimestampOffset(), ((StartpointTimestamp) startpointFromStore).getTimestampOffset());
    Assert.assertTrue(startpointFromStore.getCreationTimestamp() <= Instant.now().toEpochMilli());
    startpointFromStore = startpointManager.readStartpoint(ssp, taskName).get();
    Assert.assertEquals(StartpointTimestamp.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint2.getTimestampOffset(), ((StartpointTimestamp) startpointFromStore).getTimestampOffset());
    Assert.assertTrue(startpointFromStore.getCreationTimestamp() <= Instant.now().toEpochMilli());

    // Test overwrites
    startpointManager.writeStartpoint(ssp, startpoint3);
    startpointManager.writeStartpoint(ssp, taskName, startpoint4);
    startpointFromStore = startpointManager.readStartpoint(ssp).get();
    Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint3.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());
    Assert.assertTrue(startpointFromStore.getCreationTimestamp() <= Instant.now().toEpochMilli());
    startpointFromStore = startpointManager.readStartpoint(ssp, taskName).get();
    Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
    Assert.assertEquals(startpoint4.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());
    Assert.assertTrue(startpointFromStore.getCreationTimestamp() <= Instant.now().toEpochMilli());

    // Test deletes on SSP keys does not affect SSP+TaskName keys
    startpointManager.deleteStartpoint(ssp);
    Assert.assertFalse(startpointManager.readStartpoint(ssp).isPresent());
    Assert.assertTrue(startpointManager.readStartpoint(ssp, taskName).isPresent());

    // Test deletes on SSP+TaskName keys does not affect SSP keys
    startpointManager.writeStartpoint(ssp, startpoint3);
    startpointManager.deleteStartpoint(ssp, taskName);
    Assert.assertFalse(startpointManager.readStartpoint(ssp, taskName).isPresent());
    Assert.assertTrue(startpointManager.readStartpoint(ssp).isPresent());
  }

  @Test
  public void testFanOutBasic() throws IOException {
    SystemStreamPartition sspBroadcast = new SystemStreamPartition("mockSystem1", "mockStream1", new Partition(2));
    SystemStreamPartition sspSingle = new SystemStreamPartition("mockSystem2", "mockStream2", new Partition(3));

    TaskName taskWithNonBroadcast = new TaskName("t1");

    List<TaskName> tasks =
        ImmutableList.of(new TaskName("t0"), taskWithNonBroadcast, new TaskName("t2"), new TaskName("t3"), new TaskName("t4"), new TaskName("t5"));

    Map<TaskName, Set<SystemStreamPartition>> taskToSSPs = tasks.stream()
        .collect(Collectors
            .toMap(task -> task, task -> task.equals(taskWithNonBroadcast) ? ImmutableSet.of(sspBroadcast, sspSingle) : ImmutableSet.of(sspBroadcast)));

    StartpointSpecific startpoint42 = new StartpointSpecific("42");

    startpointManager.writeStartpoint(sspBroadcast, startpoint42);
    startpointManager.writeStartpoint(sspSingle, startpoint42);

    // startpoint42 should remap with key sspBroadcast to all tasks + sspBroadcast
    Map<TaskName, Map<SystemStreamPartition, Startpoint>> tasksFannedOutTo = startpointManager.fanOut(taskToSSPs);
    Assert.assertEquals(tasks.size(), tasksFannedOutTo.size());
    Assert.assertTrue(tasksFannedOutTo.keySet().containsAll(tasks));
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(sspBroadcast).isPresent());
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(sspSingle).isPresent());

    for (TaskName taskName : tasks) {
      Map<SystemStreamPartition, Startpoint> fanOutForTask = startpointManager.getFanOutForTask(taskName);
      if (taskName.equals(taskWithNonBroadcast)) {
        // Non-broadcast startpoint should be fanned out to only one task
        Assert.assertEquals("Should have broadcast and non-broadcast SSP", 2, fanOutForTask.size());
      } else {
        Assert.assertEquals("Should only have broadcast SSP", 1, fanOutForTask.size());
      }

      // Broadcast SSP should be on every task
      Startpoint startpointFromStore = fanOutForTask.get(sspBroadcast);
      Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
      Assert.assertEquals(startpoint42.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());

      // startpoint mapped only to task "t1" for Non-broadcast SSP
      startpointFromStore = fanOutForTask.get(sspSingle);
      if (taskName.equals(taskWithNonBroadcast)) {
        Assert.assertEquals(StartpointSpecific.class, startpointFromStore.getClass());
        Assert.assertEquals(startpoint42.getSpecificOffset(), ((StartpointSpecific) startpointFromStore).getSpecificOffset());
      } else {
        Assert.assertNull("Should not have non-broadcast SSP", startpointFromStore);
      }

      startpointManager.removeFanOutForTask(taskName);
      Assert.assertTrue(startpointManager.getFanOutForTask(taskName).isEmpty());
    }
  }

  @Test
  public void testFanOutWithStartpointResolutions() throws IOException {
    SystemStreamPartition sspBroadcast = new SystemStreamPartition("mockSystem1", "mockStream1", new Partition(2));
    SystemStreamPartition sspSingle = new SystemStreamPartition("mockSystem2", "mockStream2", new Partition(3));

    List<TaskName> tasks =
        ImmutableList.of(new TaskName("t0"), new TaskName("t1"), new TaskName("t2"), new TaskName("t3"), new TaskName("t4"));

    TaskName taskWithNonBroadcast = tasks.get(1);
    TaskName taskBroadcastInPast = tasks.get(2);
    TaskName taskBroadcastInFuture = tasks.get(3);

    Map<TaskName, Set<SystemStreamPartition>> taskToSSPs = tasks.stream()
        .collect(Collectors
            .toMap(task -> task, task -> task.equals(taskWithNonBroadcast) ? ImmutableSet.of(sspBroadcast, sspSingle) : ImmutableSet.of(sspBroadcast)));

    Instant now = Instant.now();
    StartpointMock startpointPast = new StartpointMock(now.minusMillis(10000L).toEpochMilli());
    StartpointMock startpointPresent = new StartpointMock(now.toEpochMilli());
    StartpointMock startpointFuture = new StartpointMock(now.plusMillis(10000L).toEpochMilli());

    startpointManager.getObjectMapper().registerSubtypes(StartpointMock.class);
    startpointManager.writeStartpoint(sspSingle, startpointPast);
    startpointManager.writeStartpoint(sspSingle, startpointPresent);
    startpointManager.writeStartpoint(sspBroadcast, startpointPresent);
    startpointManager.writeStartpoint(sspBroadcast, taskBroadcastInPast, startpointPast);
    startpointManager.writeStartpoint(sspBroadcast, taskBroadcastInFuture, startpointFuture);

    Map<TaskName, Map<SystemStreamPartition, Startpoint>> fannedOut = startpointManager.fanOut(taskToSSPs);
    Assert.assertEquals(tasks.size(), fannedOut.size());
    Assert.assertTrue(fannedOut.keySet().containsAll(tasks));
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(sspBroadcast).isPresent());
    for (TaskName taskName : fannedOut.keySet()) {
      Assert.assertFalse("Should be deleted after fan out for task: " + taskName.getTaskName(), startpointManager.readStartpoint(sspBroadcast, taskName).isPresent());
    }
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(sspSingle).isPresent());

    for (TaskName taskName : tasks) {
      Map<SystemStreamPartition, Startpoint> fanOutForTask = startpointManager.getFanOutForTask(taskName);
      if (taskName.equals(taskWithNonBroadcast)) {
        Assert.assertEquals(startpointPresent, fanOutForTask.get(sspSingle));
        Assert.assertEquals(startpointPresent, fanOutForTask.get(sspBroadcast));
      } else if (taskName.equals(taskBroadcastInPast)) {
        Assert.assertNull(fanOutForTask.get(sspSingle));
        Assert.assertEquals(startpointPresent, fanOutForTask.get(sspBroadcast));
      } else if (taskName.equals(taskBroadcastInFuture)) {
        Assert.assertNull(fanOutForTask.get(sspSingle));
        Assert.assertEquals(startpointFuture, fanOutForTask.get(sspBroadcast));
      } else {
        Assert.assertNull(fanOutForTask.get(sspSingle));
        Assert.assertEquals(startpointPresent, fanOutForTask.get(sspBroadcast));
      }
      startpointManager.removeFanOutForTask(taskName);
      Assert.assertTrue(startpointManager.getFanOutForTask(taskName).isEmpty());
    }
  }

  @Test
  public void testRemoveAllFanOuts() throws IOException {
    SystemStreamPartition sspBroadcast = new SystemStreamPartition("mockSystem1", "mockStream1", new Partition(2));
    SystemStreamPartition sspSingle = new SystemStreamPartition("mockSystem2", "mockStream2", new Partition(3));

    TaskName taskWithNonBroadcast = new TaskName("t1");

    List<TaskName> tasks =
        ImmutableList.of(new TaskName("t0"), taskWithNonBroadcast, new TaskName("t2"), new TaskName("t3"), new TaskName("t4"), new TaskName("t5"));

    Map<TaskName, Set<SystemStreamPartition>> taskToSSPs = tasks.stream()
        .collect(Collectors
            .toMap(task -> task, task -> task.equals(taskWithNonBroadcast) ? ImmutableSet.of(sspBroadcast, sspSingle) : ImmutableSet.of(sspBroadcast)));

    StartpointSpecific startpoint42 = new StartpointSpecific("42");

    startpointManager.writeStartpoint(sspBroadcast, startpoint42);
    startpointManager.writeStartpoint(sspSingle, startpoint42);

    // startpoint42 should remap with key sspBroadcast to all tasks + sspBroadcast
    Map<TaskName, Map<SystemStreamPartition, Startpoint>> tasksFannedOutTo = startpointManager.fanOut(taskToSSPs);
    Assert.assertEquals(tasks.size(), tasksFannedOutTo.size());
    Assert.assertTrue(tasksFannedOutTo.keySet().containsAll(tasks));
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(sspBroadcast).isPresent());
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(sspSingle).isPresent());

    startpointManager.removeAllFanOuts();

    // Write back to ensure removing all fan outs doesn't remove all startpoints
    startpointManager.writeStartpoint(sspBroadcast, startpoint42);
    startpointManager.writeStartpoint(sspSingle, startpoint42);

    Assert.assertEquals(0, startpointManager.getFanOutStore().all().size());
    Assert.assertTrue("Should not be deleted after remove all fan outs", startpointManager.readStartpoint(sspBroadcast).isPresent());
    Assert.assertTrue("Should not be deleted after remove all fan outs", startpointManager.readStartpoint(sspSingle).isPresent());
  }

  @Test
  public void testRemoveFanOutForTaskSSPs() throws Exception {
    SystemStreamPartition ssp0 = new SystemStreamPartition("mockSystem", "mockStream", new Partition(0));
    SystemStreamPartition ssp1 = new SystemStreamPartition("mockSystem", "mockStream", new Partition(1));
    TaskName taskName = new TaskName("mockTask");
    Map<TaskName, Set<SystemStreamPartition>> taskToSSPs = ImmutableMap.of(taskName, ImmutableSet.of(ssp0, ssp1));
    StartpointSpecific startpoint42 = new StartpointSpecific("42");
    startpointManager.writeStartpoint(ssp0, startpoint42);
    startpointManager.writeStartpoint(ssp1, startpoint42);
    Map<TaskName, Map<SystemStreamPartition, Startpoint>> tasksFannedOutTo = startpointManager.fanOut(taskToSSPs);
    Assert.assertEquals(ImmutableSet.of(taskName), tasksFannedOutTo.keySet());
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(ssp0).isPresent());
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(ssp1).isPresent());

    // no action takes if not specify any system stream partition
    startpointManager.removeFanOutForTaskSSPs(taskName, ImmutableSet.of());
    Assert.assertEquals(ImmutableMap.of(ssp0, startpoint42, ssp1, startpoint42), startpointManager.getFanOutForTask(taskName));

    // partially removal: remove the fanned out startpoint for the specified system stream partition only
    startpointManager.removeFanOutForTaskSSPs(taskName, ImmutableSet.of(ssp0));
    Assert.assertEquals(ImmutableMap.of(ssp1, startpoint42), startpointManager.getFanOutForTask(taskName));

    // remove the whole task's startpoints if all the task's partitions' are removed
    startpointManager.removeFanOutForTaskSSPs(taskName, ImmutableSet.of(ssp1));
    Assert.assertEquals(ImmutableMap.of(), startpointManager.getFanOutForTask(taskName));
  }

  @Test
  public void testDeleteAllStartpoints() throws IOException {
    SystemStreamPartition sspBroadcast = new SystemStreamPartition("mockSystem1", "mockStream1", new Partition(2));
    SystemStreamPartition sspSingle = new SystemStreamPartition("mockSystem2", "mockStream2", new Partition(3));

    TaskName taskWithNonBroadcast = new TaskName("t1");

    List<TaskName> tasks =
        ImmutableList.of(new TaskName("t0"), taskWithNonBroadcast, new TaskName("t2"), new TaskName("t3"), new TaskName("t4"), new TaskName("t5"));

    Map<TaskName, Set<SystemStreamPartition>> taskToSSPs = tasks.stream()
        .collect(Collectors
            .toMap(task -> task, task -> task.equals(taskWithNonBroadcast) ? ImmutableSet.of(sspBroadcast, sspSingle) : ImmutableSet.of(sspBroadcast)));

    StartpointSpecific startpoint42 = new StartpointSpecific("42");

    startpointManager.writeStartpoint(sspBroadcast, startpoint42);
    startpointManager.writeStartpoint(sspSingle, startpoint42);

    // startpoint42 should remap with key sspBroadcast to all tasks + sspBroadcast
    Map<TaskName, Map<SystemStreamPartition, Startpoint>> tasksFannedOutTo = startpointManager.fanOut(taskToSSPs);
    Assert.assertEquals(tasks.size(), tasksFannedOutTo.size());
    Assert.assertTrue(tasksFannedOutTo.keySet().containsAll(tasks));
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(sspBroadcast).isPresent());
    Assert.assertFalse("Should be deleted after fan out", startpointManager.readStartpoint(sspSingle).isPresent());

    // Re-populate startpoints after fan out
    startpointManager.writeStartpoint(sspBroadcast, startpoint42);
    startpointManager.writeStartpoint(sspSingle, startpoint42);
    Assert.assertEquals(2, startpointManager.getReadWriteStore().all().size());

    startpointManager.deleteAllStartpoints();
    Assert.assertEquals(0, startpointManager.getReadWriteStore().all().size());

    // Fan outs should be untouched
    Assert.assertEquals(tasks.size(), startpointManager.getFanOutStore().all().size());
  }
}
