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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.Partition;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.stream.SystemStreamPartitionGrouper;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Assert;
import org.junit.Test;


public class TestStartpointManager {
  @Test
  public void TestDefaultMetadataStore() {
    MapConfig config = new MapConfig(ImmutableMap.of(JobConfig.METADATA_STORE_FACTORY(), MockMetadataStoreFactory.class.getCanonicalName()));
    StartpointManager startpointManager = StartpointManager.getInstance(config, new NoOpMetricsRegistry());
    Assert.assertNotNull(startpointManager);
    Assert.assertEquals(MockMetadataStore.class, startpointManager.getMetadataStore().getClass());
    startpointManager.stop();
  }

  @Test
  public void TestSpecifiedMetadataStore() {
    MapConfig config = new MapConfig(ImmutableMap.of(JobConfig.STARTPOINT_METADATA_STORE_FACTORY(), MockMetadataStoreFactory.class.getCanonicalName()));
    StartpointManager startpointManager = StartpointManager.getInstance(config, new NoOpMetricsRegistry());
    Assert.assertNotNull(startpointManager);
    Assert.assertEquals(MockMetadataStore.class, startpointManager.getMetadataStore().getClass());
    startpointManager.stop();
  }

  @Test
  public void TestNoMetadataStore() {
    Assert.assertNull(StartpointManager.getInstance(new MapConfig(), new NoOpMetricsRegistry()));
  }

  @Test
  public void TestNoLongerUsableAfterStop() {
    MapConfig config = new MapConfig(ImmutableMap.of(JobConfig.METADATA_STORE_FACTORY(), MockMetadataStoreFactory.class.getCanonicalName()));
    StartpointManager startpointManager = StartpointManager.getInstance(config, new NoOpMetricsRegistry());
    SystemStreamPartition ssp =
        new SystemStreamPartition("mockSystem", "mockStream", new Partition(2));
    TaskName taskName = new TaskName("MockTask");
    Startpoint startpoint = Startpoint.withEarliest();

    startpointManager.stop();

    try {
      startpointManager.writeStartpoint(ssp, startpoint);
      Assert.fail("Expected precondition exception.");
    } catch (NullPointerException ex) { }

    try {
      startpointManager.writeStartpointForTask(ssp, taskName, startpoint);
      Assert.fail("Expected precondition exception.");
    } catch (NullPointerException ex) { }

    try {
      startpointManager.readStartpoint(ssp);
      Assert.fail("Expected precondition exception.");
    } catch (NullPointerException ex) { }

    try {
      startpointManager.readStartpointForTask(ssp, taskName);
      Assert.fail("Expected precondition exception.");
    } catch (NullPointerException ex) { }

    try {
      startpointManager.deleteStartpoint(ssp);
      Assert.fail("Expected precondition exception.");
    } catch (NullPointerException ex) { }

    try {
      startpointManager.deleteStartpointForTask(ssp, taskName);
      Assert.fail("Expected precondition exception.");
    } catch (NullPointerException ex) { }

    List<TaskName> tasks =
        ImmutableList.of(new TaskName("t0"), new TaskName("t1"));
    MockSSPGrouper mockSSPGrouper = new MockSSPGrouper(ImmutableSet.copyOf(tasks));
    try {
      startpointManager.groupStartpointsPerTask(ssp, mockSSPGrouper);
      Assert.fail("Expected precondition exception.");
    } catch (NullPointerException ex) { }
  }

  @Test
  public void TestBasics() {
    MapConfig config = new MapConfig(ImmutableMap.of(JobConfig.METADATA_STORE_FACTORY(), MockMetadataStoreFactory.class.getCanonicalName()));
    StartpointManager startpointManager = StartpointManager.getInstance(config, new NoOpMetricsRegistry());
    SystemStreamPartition ssp =
        new SystemStreamPartition("mockSystem", "mockStream", new Partition(2));
    TaskName taskName = new TaskName("MockTask");
    Startpoint startpoint1 = Startpoint.withTimestamp(111111111L);
    Startpoint startpoint2 = Startpoint.withTimestamp(222222222L);
    Startpoint startpoint3 = Startpoint.withTimestamp(333333333L);
    Startpoint startpoint4 = Startpoint.withTimestamp(444444444L);

    // Test storedAt field is null by default
    Assert.assertNull(startpoint1.getStoredAt());
    Assert.assertNull(startpoint2.getStoredAt());
    Assert.assertNull(startpoint3.getStoredAt());
    Assert.assertNull(startpoint4.getStoredAt());

    // Test reads on non-existent keys
    Assert.assertNull(startpointManager.readStartpoint(ssp));
    Assert.assertNull(startpointManager.readStartpointForTask(ssp, taskName));

    // Test writes
    startpointManager.writeStartpoint(ssp, startpoint1);
    startpointManager.writeStartpointForTask(ssp, taskName, startpoint2);
    Assert.assertEquals(startpoint1.getPosition(), startpointManager.readStartpoint(ssp).getPosition());
    Assert.assertEquals(startpoint2.getPosition(), startpointManager.readStartpointForTask(ssp, taskName).getPosition());
    // Test storedAt field is set when written to metadata store
    Assert.assertTrue(startpointManager.readStartpoint(ssp).getStoredAt() > 0);
    Assert.assertTrue(startpointManager.readStartpointForTask(ssp, taskName).getStoredAt() > 0);

    // Test overwrites
    startpointManager.writeStartpoint(ssp, startpoint3);
    startpointManager.writeStartpointForTask(ssp, taskName, startpoint4);
    Assert.assertEquals(startpoint3.getPosition(), startpointManager.readStartpoint(ssp).getPosition());
    Assert.assertEquals(startpoint4.getPosition(), startpointManager.readStartpointForTask(ssp, taskName).getPosition());
    // Test storedAt field is set when written to metadata store
    Assert.assertTrue(startpointManager.readStartpoint(ssp).getStoredAt() > 0);
    Assert.assertTrue(startpointManager.readStartpointForTask(ssp, taskName).getStoredAt() > 0);

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
  public void TestGroupStartpointsPerTask() {
    MapConfig config = new MapConfig(ImmutableMap.of(JobConfig.METADATA_STORE_FACTORY(), MockMetadataStoreFactory.class.getCanonicalName()));
    StartpointManager startpointManager = StartpointManager.getInstance(config, new NoOpMetricsRegistry());
    SystemStreamPartition ssp1 =
        new SystemStreamPartition("mockSystem1", "mockStream1", new Partition(2));
    SystemStreamPartition ssp2 =
        new SystemStreamPartition("mockSystem2", "mockStream2", new Partition(3));

    List<TaskName> tasks =
        ImmutableList.of(new TaskName("t0"), new TaskName("t1"), new TaskName("t2"), new TaskName("t3"), new TaskName("t4"), new TaskName("t5"));
    MockSSPGrouper mockSSPGrouper = new MockSSPGrouper(ImmutableSet.copyOf(tasks));

    Startpoint startpoint42 = Startpoint.withSpecificOffset("42");

    startpointManager.writeStartpoint(ssp1, startpoint42);

    // startpoint42 should remap with key ssp1 to all tasks + ssp1

    startpointManager.groupStartpointsPerTask(ssp1, mockSSPGrouper);

    for (TaskName taskName : tasks) {
      Assert.assertEquals(startpoint42.getPosition(), startpointManager.readStartpointForTask(ssp1, taskName).getPosition());
    }
    Assert.assertNull(startpointManager.readStartpoint(ssp1));

    // Test startpoints that were explicit assigned to an SSP+TaskName will not be overwritten from groupStartpointsPerTask

    Startpoint startpoint1024 = Startpoint.withSpecificOffset("1024");

    startpointManager.writeStartpoint(ssp2, startpoint42);
    startpointManager.writeStartpointForTask(ssp2, tasks.get(1), startpoint1024);
    startpointManager.writeStartpointForTask(ssp2, tasks.get(3), startpoint1024);

    Set<TaskName> tasksResult = startpointManager.groupStartpointsPerTask(ssp2, mockSSPGrouper);
    Assert.assertEquals(tasks.size(), tasksResult.size());
    Assert.assertTrue(tasksResult.containsAll(tasks));

    Assert.assertEquals(startpoint42.getPosition(), startpointManager.readStartpointForTask(ssp2, tasks.get(0)).getPosition());
    Assert.assertEquals(startpoint1024.getPosition(), startpointManager.readStartpointForTask(ssp2, tasks.get(1)).getPosition());
    Assert.assertEquals(startpoint42.getPosition(), startpointManager.readStartpointForTask(ssp2, tasks.get(2)).getPosition());
    Assert.assertEquals(startpoint1024.getPosition(), startpointManager.readStartpointForTask(ssp2, tasks.get(3)).getPosition());
    Assert.assertEquals(startpoint42.getPosition(), startpointManager.readStartpointForTask(ssp2, tasks.get(4)).getPosition());
    Assert.assertEquals(startpoint42.getPosition(), startpointManager.readStartpointForTask(ssp2, tasks.get(5)).getPosition());
    Assert.assertNull(startpointManager.readStartpoint(ssp2));

    startpointManager.stop();
  }

  @Test
  public void TestStartpointKey() {
    SystemStreamPartition ssp1 = new SystemStreamPartition("system", "stream", new Partition(2));
    SystemStreamPartition ssp2 = new SystemStreamPartition("system", "stream", new Partition(3));

    StartpointKey startpointKey1 = new StartpointKey(ssp1);
    StartpointKey startpointKey2 = new StartpointKey(ssp1);
    StartpointKey startpointKeyWithDifferentSSP = new StartpointKey(ssp2);
    StartpointKey startpointKeyWithTask1 = new StartpointKey(ssp1, new TaskName("t1"));
    StartpointKey startpointKeyWithTask2 = new StartpointKey(ssp1, new TaskName("t1"));
    StartpointKey startpointKeyWithDifferentTask = new StartpointKey(ssp1, new TaskName("t2"));

    Assert.assertEquals(startpointKey1, startpointKey2);
    Assert.assertEquals(startpointKey1.toString(), startpointKey2.toString());
    Assert.assertEquals(startpointKeyWithTask1, startpointKeyWithTask2);
    Assert.assertEquals(startpointKeyWithTask1.toString(), startpointKeyWithTask2.toString());

    Assert.assertNotEquals(startpointKey1, startpointKeyWithTask1);
    Assert.assertNotEquals(startpointKey1.toString(), startpointKeyWithTask1.toString());

    Assert.assertNotEquals(startpointKey1, startpointKeyWithDifferentSSP);
    Assert.assertNotEquals(startpointKey1.toString(), startpointKeyWithDifferentSSP.toString());
    Assert.assertNotEquals(startpointKeyWithTask1, startpointKeyWithDifferentTask);
    Assert.assertNotEquals(startpointKeyWithTask1.toString(), startpointKeyWithDifferentTask.toString());

    Assert.assertNotEquals(startpointKeyWithTask1, startpointKeyWithDifferentTask);
    Assert.assertNotEquals(startpointKeyWithTask1.toString(), startpointKeyWithDifferentTask.toString());
  }

  @Test
  public void TestStartpointSerde() {
    StartpointSerde startpointSerde = new StartpointSerde();

    Startpoint startpoint1 = Startpoint.withSpecificOffset("123");
    Startpoint startpoint2 = Startpoint.withSpecificOffset("123");
    Assert.assertEquals(startpoint1, startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertNotEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2.copyWithStoredAt(1234567L))));

    startpoint1 = Startpoint.withTimestamp(2222222L);
    startpoint2 = Startpoint.withTimestamp(2222222L);
    Assert.assertEquals(startpoint1, startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertNotEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2.copyWithStoredAt(1234567L))));

    startpoint1 = Startpoint.withEarliest();
    startpoint2 = Startpoint.withEarliest();
    Assert.assertEquals(startpoint1, startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertNotEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2.copyWithStoredAt(1234567L))));

    startpoint1 = Startpoint.withLatest();
    startpoint2 = Startpoint.withLatest();
    Assert.assertEquals(startpoint1, startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertNotEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2.copyWithStoredAt(1234567L))));

    startpoint1 = Startpoint.withBootstrap("Bootstrap info");
    startpoint2 = Startpoint.withBootstrap("Bootstrap info");
    Assert.assertEquals(startpoint1, startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertNotEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2)));
    Assert.assertEquals(startpoint1.copyWithStoredAt(1234567L), startpointSerde.fromBytes(startpointSerde.toBytes(startpoint2.copyWithStoredAt(1234567L))));
  }

  static class MockSSPGrouper implements SystemStreamPartitionGrouper {
    private Set<TaskName> mockTasks;

    public MockSSPGrouper(Set<TaskName> mockTasks) {
      this.mockTasks = mockTasks;
    }

    @Override
    public Map<TaskName, Set<SystemStreamPartition>> group(Set<SystemStreamPartition> ssps) {
      return mockTasks.stream().collect(Collectors.toMap(task -> task, task -> ssps));
    }
  }
}
