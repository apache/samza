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
package org.apache.samza.elasticity.util;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.Partition;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.checkpoint.CheckpointId;
import org.apache.samza.checkpoint.CheckpointV2;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamPartition;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;


// #TODO: going to make this entire class parametrized.
public class TestElasticityUtils {
  private static final TaskName TASKNAME_GROUP_BY_PARTITION = new TaskName("Partition 0");
  private static final TaskName ELASTIC_TASKNAME_GROUP_BY_PARTITION = new TaskName("Partition 0_1_2");
  private static final TaskName TASKNAME_GROUP_BY_SSP = new TaskName("SystemStreamPartition [systemA, streamB, 0]");
  private static final TaskName ELASTIC_TASKNAME_GROUP_BY_SSP = new TaskName("SystemStreamPartition [systemA, streamB, 0, 1]_2");

  @Test
  public void testComputeLastProcessedOffsetsFromCheckpointMap() {
    // Setup :
    // there is one ssp = SystemStreamPartition [systemA, streamB, partition(0)] consumed by the job
    // Note: Partition 0_1_2 means task consumes keyBucket 1 of partition 0 and has elasticityFactor 2.
    // Before elasticity, job has one task with name "Partition 0"
    // with elasticity factor 2, job has 2 tasks with names "Partition 0_0_2" and "Partition 0_1_2"
    //         Partition 0_0_2 consumes SSP[systemA, stream B, partition(0), keyBucket(0)]
    //         Partition 0_1_2 consumes SSP[systemA, stream B, partition(0), keyBucket(1)]
    // with elasticity factor 4, job has 4 tasks with names "Partition 0_0_4", "Partition 0_1_4", "Partition 0_2_4" and "Partition 0_3_4"
    //         Partition 0_0_4 consumes SSP[systemA, stream B, partition(0), keyBucket(0)]
    //         Partition 0_1_4 consumes SSP[systemA, stream B, partition(0), keyBucket(1)]
    //         Partition 0_2_4 consumes SSP[systemA, stream B, partition(0), keyBucket(2)]
    //         Partition 0_3_4 consumes SSP[systemA, stream B, partition(0), keyBucket(3)]

    //
    // From the definition of keyBucket computation using elasticity factor in
    // {@link IncomingMessageEnvelope.getSystemStresamPartition(elasticityFactor) as
    // keyBucket = (Math.abs(envelopeKeyorOffset.hashCode()) % 31) % elasticityFactor
    // messages processed by 0_0_4 and 0_2_4 will be the same as those processed by 0_0_2
    // messages processed by 0_1_4 and 0_3_4 will be the same as those processed by 0_1_2
    // messages processed by 0_0_2 and 0_1_2 will be the same as those processed by Partition 0 itself

    TaskName taskName = new TaskName("Partition 0_0_2");
    Map<TaskName, Checkpoint> checkpointMap = new HashMap<>();
    SystemStreamPartition ssp = new SystemStreamPartition("systemA", "streamB", new Partition(0));
    SystemStreamPartition ssp0 = new SystemStreamPartition("systemA", "streamB", new Partition(0), 0);
    SystemStreamPartition ssp2 = new SystemStreamPartition("systemA", "streamB", new Partition(0), 2);


    SystemAdmin mockSystemAdmin = Mockito.mock(SystemAdmin.class);
    // offsets ordering 1 < 2 < 3 < 4
    Mockito.when(mockSystemAdmin.offsetComparator("1", "2")).thenReturn(-1);
    Mockito.when(mockSystemAdmin.offsetComparator("2", "1")).thenReturn(1);
    Mockito.when(mockSystemAdmin.offsetComparator("1", "3")).thenReturn(-1);
    Mockito.when(mockSystemAdmin.offsetComparator("3", "1")).thenReturn(1);
    Mockito.when(mockSystemAdmin.offsetComparator("1", "4")).thenReturn(-1);
    Mockito.when(mockSystemAdmin.offsetComparator("4", "1")).thenReturn(1);
    Mockito.when(mockSystemAdmin.offsetComparator("2", "3")).thenReturn(-1);
    Mockito.when(mockSystemAdmin.offsetComparator("3", "2")).thenReturn(1);
    Mockito.when(mockSystemAdmin.offsetComparator("2", "4")).thenReturn(-1);
    Mockito.when(mockSystemAdmin.offsetComparator("4", "2")).thenReturn(1);
    Mockito.when(mockSystemAdmin.offsetComparator("3", "4")).thenReturn(-1);
    Mockito.when(mockSystemAdmin.offsetComparator("4", "3")).thenReturn(1);

    SystemAdmins mockSystemAdmins = Mockito.mock(SystemAdmins.class);
    Mockito.when(mockSystemAdmins.getSystemAdmin(ssp0.getSystem())).thenReturn(mockSystemAdmin);

    // case 1: for task Partition 0_0_2: last deploy was with ef = 2 itself.
    // hence "Partition 0_0_2" has the largest offset and that should be used for computing checkpoint for 0_0_2 now also
    checkpointMap.put(new TaskName("Partition 0"), buildCheckpointV2(ssp, "1"));
    checkpointMap.put(new TaskName("Partition 0_0_2"), buildCheckpointV2(ssp0, "4"));
    checkpointMap.put(new TaskName("Partition 0_0_4"), buildCheckpointV2(ssp0, "2"));
    checkpointMap.put(new TaskName("Partition 0_2_4"), buildCheckpointV2(ssp2, "3"));
    Map<SystemStreamPartition, String> result = ElasticityUtils.computeLastProcessedOffsetsFromCheckpointMap(
        taskName, Collections.singleton(ssp0), checkpointMap, mockSystemAdmins);
    Assert.assertEquals("4", result.get(ssp0));

    // case 2: for task Partition 0_0_2: last deploy was with ef =1
    // hence "Partition 0" has the largest offset. Computing checkpint for 0_0_2 should use this largest offset
    checkpointMap = new HashMap<>();
    checkpointMap.put(new TaskName("Partition 0"), buildCheckpointV2(ssp, "4"));
    checkpointMap.put(new TaskName("Partition 0_0_2"), buildCheckpointV2(ssp0, "1"));
    checkpointMap.put(new TaskName("Partition 0_0_4"), buildCheckpointV2(ssp0, "3"));
    checkpointMap.put(new TaskName("Partition 0_2_4"), buildCheckpointV2(ssp2, "2"));


    result = ElasticityUtils.computeLastProcessedOffsetsFromCheckpointMap(
        taskName, Collections.singleton(ssp0), checkpointMap, mockSystemAdmins);
    Assert.assertEquals("4", result.get(ssp0));


    // case 3: for task partition 0_0_2: last deploy was with ef = 4
    // hence checkpoints of Partition 0_0_4 and Partition 0_3_4 are relevant.
    // since messages from both end up in 0_0_2 with ef=2, need to take min of their checkpointed offsets

    checkpointMap.put(new TaskName("Partition 0"), buildCheckpointV2(ssp, "1"));
    checkpointMap.put(new TaskName("Partition 0_0_2"), buildCheckpointV2(ssp0, "2"));
    checkpointMap.put(new TaskName("Partition 0_0_4"), buildCheckpointV2(ssp0, "3"));
    checkpointMap.put(new TaskName("Partition 0_2_4"), buildCheckpointV2(ssp2, "4"));
    result = ElasticityUtils.computeLastProcessedOffsetsFromCheckpointMap(
        taskName, Collections.singleton(ssp0), checkpointMap, mockSystemAdmins);
    Assert.assertEquals("3", result.get(ssp0));
  }
  @Test
  public void testComputeLastProcessedOffsetsWithEdgeCases() {
    TaskName taskName = new TaskName("Partition 0_0_2");
    Map<TaskName, Checkpoint> checkpointMap = new HashMap<>();
    SystemStreamPartition ssp0 = new SystemStreamPartition("systemA", "streamB", new Partition(0), 0);

    SystemAdmin mockSystemAdmin = Mockito.mock(SystemAdmin.class);
    SystemAdmins mockSystemAdmins = Mockito.mock(SystemAdmins.class);
    Mockito.when(mockSystemAdmins.getSystemAdmin(ssp0.getSystem())).thenReturn(mockSystemAdmin);

    // case 1: empty checkpoint map
    Map<SystemStreamPartition, String> result = ElasticityUtils.computeLastProcessedOffsetsFromCheckpointMap(
        taskName, Collections.singleton(ssp0), checkpointMap, mockSystemAdmins);
    Assert.assertTrue("if given checkpoint map is empty, return empty last processed offsets map", result.isEmpty());

    // case 2: null checkpoints given for some ancestor tasks
    checkpointMap.put(new TaskName("Partition 0"), null);
    result = ElasticityUtils.computeLastProcessedOffsetsFromCheckpointMap(
        taskName, Collections.singleton(ssp0), checkpointMap, mockSystemAdmins);
    Assert.assertTrue("if given checkpoint map has null checkpoint, return empty last processed offsets map", result.isEmpty());

    // case 3: null checkpoints given for some descendant tasks
    checkpointMap.put(new TaskName("Partition 0_0_4"), null);
    result = ElasticityUtils.computeLastProcessedOffsetsFromCheckpointMap(
        taskName, Collections.singleton(ssp0), checkpointMap, mockSystemAdmins);
    Assert.assertTrue("if given checkpoint map has null checkpoint, return empty last processed offsets map", result.isEmpty());
  }

  @Test
  public void testTaskIsGroupByPartitionOrGroupBySSP() {
    String msgPartition = "GroupByPartition task should start with Partition";
    String msgSsp = "GroupBySystemStreamPartition task should start with SystemStreamPartition";

    Assert.assertTrue(msgPartition, ElasticityUtils.isGroupByPartitionTask(TASKNAME_GROUP_BY_PARTITION));
    Assert.assertFalse(msgPartition, ElasticityUtils.isGroupBySystemStreamPartitionTask(TASKNAME_GROUP_BY_PARTITION));

    Assert.assertTrue(msgPartition, ElasticityUtils.isGroupByPartitionTask(ELASTIC_TASKNAME_GROUP_BY_PARTITION));
    Assert.assertFalse(msgPartition, ElasticityUtils.isGroupBySystemStreamPartitionTask(
        ELASTIC_TASKNAME_GROUP_BY_PARTITION));

    Assert.assertTrue(msgSsp, ElasticityUtils.isGroupBySystemStreamPartitionTask(TASKNAME_GROUP_BY_SSP));
    Assert.assertFalse(msgSsp, ElasticityUtils.isGroupByPartitionTask(TASKNAME_GROUP_BY_SSP));

    Assert.assertTrue(msgSsp, ElasticityUtils.isGroupBySystemStreamPartitionTask(ELASTIC_TASKNAME_GROUP_BY_SSP));
    Assert.assertFalse(msgSsp, ElasticityUtils.isGroupByPartitionTask(ELASTIC_TASKNAME_GROUP_BY_SSP));

    TaskName taskName = new TaskName("FooBar");
    Assert.assertFalse(msgPartition, ElasticityUtils.isGroupByPartitionTask(taskName));
    Assert.assertFalse(msgSsp, ElasticityUtils.isGroupBySystemStreamPartitionTask(taskName));
  }

  @Test
  public void testIsTaskNameElastic() {
    Assert.assertFalse(ElasticityUtils.isTaskNameElastic(TASKNAME_GROUP_BY_SSP));
    Assert.assertTrue(ElasticityUtils.isTaskNameElastic(ELASTIC_TASKNAME_GROUP_BY_SSP));
    Assert.assertFalse(ElasticityUtils.isTaskNameElastic(TASKNAME_GROUP_BY_PARTITION));
    Assert.assertTrue(ElasticityUtils.isTaskNameElastic(ELASTIC_TASKNAME_GROUP_BY_PARTITION));
  }

  @Test
  public void testGetElasticTaskNameParts() {
    TaskNameComponents taskNameComponents = ElasticityUtils.getTaskNameParts(TASKNAME_GROUP_BY_PARTITION);
    Assert.assertEquals(taskNameComponents.partition, 0);
    Assert.assertEquals(taskNameComponents.keyBucket, TaskNameComponents.DEFAULT_KEY_BUCKET);
    Assert.assertEquals(taskNameComponents.elasticityFactor, TaskNameComponents.DEFAULT_ELASTICITY_FACTOR);

    taskNameComponents = ElasticityUtils.getTaskNameParts(ELASTIC_TASKNAME_GROUP_BY_PARTITION);
    Assert.assertEquals(taskNameComponents.partition, 0);
    Assert.assertEquals(taskNameComponents.keyBucket, 1);
    Assert.assertEquals(taskNameComponents.elasticityFactor, 2);

    taskNameComponents = ElasticityUtils.getTaskNameParts(TASKNAME_GROUP_BY_SSP);
    Assert.assertEquals(taskNameComponents.system, "systemA");
    Assert.assertEquals(taskNameComponents.stream, "streamB");
    Assert.assertEquals(taskNameComponents.partition, 0);
    Assert.assertEquals(taskNameComponents.keyBucket, TaskNameComponents.DEFAULT_KEY_BUCKET);
    Assert.assertEquals(taskNameComponents.elasticityFactor, TaskNameComponents.DEFAULT_ELASTICITY_FACTOR);

    taskNameComponents = ElasticityUtils.getTaskNameParts(ELASTIC_TASKNAME_GROUP_BY_SSP);
    Assert.assertEquals(taskNameComponents.system, "systemA");
    Assert.assertEquals(taskNameComponents.stream, "streamB");
    Assert.assertEquals(taskNameComponents.partition, 0);
    Assert.assertEquals(taskNameComponents.keyBucket, 1);
    Assert.assertEquals(taskNameComponents.elasticityFactor, 2);

    taskNameComponents = ElasticityUtils.getTaskNameParts(new TaskName("FooBar"));
    Assert.assertEquals(taskNameComponents.partition, TaskNameComponents.INVALID_PARTITION);
  }

  @Test
  public void testIsOtherTaskAncestorDescendantOfCurrentTask() {
    TaskName task0 = new TaskName("Partition 0");
    TaskName task1 = new TaskName("Partition 1");
    TaskName task002 = new TaskName("Partition 0_0_2");
    TaskName task012 = new TaskName("Partition 0_1_2");
    TaskName task004 = new TaskName("Partition 0_0_4");
    TaskName task014 = new TaskName("Partition 0_1_4");
    TaskName task024 = new TaskName("Partition 0_2_4");
    TaskName task034 = new TaskName("Partition 0_3_4");

    TaskName sspTask0 = new TaskName("SystemStreamPartition [systemA, streamB, 0]");
    TaskName sspTask002 = new TaskName("SystemStreamPartition [systemA, streamB, 0, 0]_2");
    TaskName sspTask012 = new TaskName("SystemStreamPartition [systemA, streamB, 0, 1]_2");
    TaskName sspTask004 = new TaskName("SystemStreamPartition [systemA, streamB, 0, 0]_4");
    TaskName sspTask014 = new TaskName("SystemStreamPartition [systemA, streamB, 0, 1]_4");
    TaskName sspTask024 = new TaskName("SystemStreamPartition [systemA, streamB, 0, 2]_4");
    TaskName sspTask034 = new TaskName("SystemStreamPartition [systemA, streamB, 0, 3]_4");

    // Partition 0 is ancestor of all tasks Partition 0_0_2, 0_1_2, 0_0_4, 0_1_4, 0_2_4, 0_3_4 and itself
    // and all these tasks are descendants of Partition 0 (except itself)
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task0, task0));
    Assert.assertFalse(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task0, task1));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task002, task0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task012, task0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task004, task0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task014, task0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task024, task0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task034, task0));

    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(task0, task002));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(task0, task012));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(task0, task004));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(task0, task014));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(task0, task024));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(task0, task034));

    // Partition 0_0_2 is ancestor of tasks Partition 0_0_4 and 0_2_4 and itself
    // these tasks are descendants of 0_0_2
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task004, task002));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task024, task002));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(task002, task002));

    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(task002, task004));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(task002, task024));

    // "SystemStreamPartition [systemA, streamB, 0]
    // is ancestor of all tasks "SystemStreamPartition [systemA, streamB, 0, 0]_2, [systemA, streamB, 0, 1]_2 and the rest incl itself
    // and all these tasks are descendants of Partition 0 (except itself)
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask0, sspTask0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask002, sspTask0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask012, sspTask0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask004, sspTask0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask014, sspTask0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask024, sspTask0));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask034, sspTask0));

    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(sspTask0, sspTask002));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(sspTask0, sspTask012));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(sspTask0, sspTask004));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(sspTask0, sspTask014));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(sspTask0, sspTask024));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(sspTask0, sspTask034));

    // SystemStreamPartition [systemA, streamB, 0, 0]_2 is ancestor of
    // tasks SystemStreamPartition [systemA, streamB, 0, 0]_4, SystemStreamPartition [systemA, streamB, 0, 2]_4 and itself
    // similarly, these tasks are descendants of SystemStreamPartition [systemA, streamB, 0, 0]_2
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask004, sspTask002));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask024, sspTask002));
    Assert.assertTrue(ElasticityUtils.isOtherTaskAncestorOfCurrentTask(sspTask002, sspTask002));

    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(sspTask002, sspTask004));
    Assert.assertTrue(ElasticityUtils.isOtherTaskDescendantOfCurrentTask(sspTask002, sspTask024));
  }

  @Test
  public void testGetAncestorAndDescendantCheckpoints() {
    TaskName taskName = new TaskName("Partition 0_0_2");
    Map<TaskName, Checkpoint> checkpointMap = new HashMap<>();
    SystemStreamPartition ssp = new SystemStreamPartition("systemA", "streamB", new Partition(0));
    Checkpoint ansCheckpoint1 = buildCheckpointV2(ssp, "1");
    Checkpoint ansCheckpoint2 = buildCheckpointV2(ssp, "2");
    Checkpoint desCheckpoint1 = buildCheckpointV2(ssp, "3");
    Checkpoint desCheckpoint2 = buildCheckpointV2(ssp, "4");
    Checkpoint unrelCheckpoint = buildCheckpointV2(ssp, "5");
    Set<Checkpoint> ansCheckpointSet = new HashSet<>(Arrays.asList(ansCheckpoint1, ansCheckpoint2));
    Set<Checkpoint> desCheckpointSet = new HashSet<>(Arrays.asList(desCheckpoint1, desCheckpoint2));

    checkpointMap.put(new TaskName("Partition 0"), ansCheckpoint1);
    checkpointMap.put(new TaskName("Partition 0_0_2"), ansCheckpoint2);
    checkpointMap.put(new TaskName("Partition 0_0_4"), desCheckpoint1);
    checkpointMap.put(new TaskName("Partition 0_2_4"), desCheckpoint2);
    checkpointMap.put(new TaskName("Partition 0_1_4"), unrelCheckpoint);

    Pair<Set<Checkpoint>, Map<Integer, Set<Checkpoint>>> result =
        ElasticityUtils.getAncestorAndDescendantCheckpoints(taskName, checkpointMap);
    Set<Checkpoint> anscestorCheckpointSet = result.getLeft();
    Set<Checkpoint> descendantCheckpointSetForEf4 = result.getRight().get(4);

    Assert.assertTrue("should contain all ancestors' checkpoints",
        anscestorCheckpointSet.containsAll(ansCheckpointSet));
    Assert.assertFalse("should not contain a descendant checkpoint in anscetor list",
        anscestorCheckpointSet.contains(desCheckpoint1));
    Assert.assertFalse("should not contain an unrelated checkpoint in ancestor list",
        anscestorCheckpointSet.contains(unrelCheckpoint));

    Assert.assertTrue("should contain all descendants' checkpoints",
        descendantCheckpointSetForEf4.containsAll(desCheckpointSet));
    Assert.assertFalse("should not contain a anscetor checkpoint in descendant list",
        descendantCheckpointSetForEf4.contains(ansCheckpoint1));
    Assert.assertFalse("should not contain an unrelated checkpoint in descendant list",
        descendantCheckpointSetForEf4.contains(unrelCheckpoint));
  }

  @Test
  public void testGetOffsetForSSPInCheckpoint() {
    String offset1 = "1111";
    String offset2 = "2222";
    // case 1: when looking for exact ssp
    SystemStreamPartition ssp = new SystemStreamPartition("systemA", "streamB", new Partition(0));
    Checkpoint checkpoint1 = buildCheckpointV2(ssp, offset1);
    Assert.assertEquals(ElasticityUtils.getOffsetForSSPInCheckpoint(checkpoint1, ssp), offset1);

    // case 2: checkpoint has ssp with key bucket but looking for the full ssp (same system stream and partition but without keybucket)
    SystemStreamPartition sspWithKB = new SystemStreamPartition("systemA", "streamB", new Partition(0), 1);
    checkpoint1 = buildCheckpointV2(sspWithKB, offset2);
    Assert.assertEquals(ElasticityUtils.getOffsetForSSPInCheckpoint(checkpoint1, ssp), offset2);

    // case 3: try getting offset for an ssp not present in the checkpoint -> should return null
    SystemStreamPartition ssp2 = new SystemStreamPartition("A", "B", new Partition(1));
    Assert.assertEquals(ElasticityUtils.getOffsetForSSPInCheckpoint(checkpoint1, ssp2), null);
  }

  @Test
  public void testGetMaxMinOffsetForSSPInCheckpointSet() {
    String offset1 = "1111";
    String offset2 = "2222";

    SystemStreamPartition ssp = new SystemStreamPartition("systemA", "streamB", new Partition(0));
    Checkpoint checkpoint1 = buildCheckpointV2(ssp, offset1);
    Checkpoint checkpoint2 = buildCheckpointV2(ssp, offset2);
    Set<Checkpoint> checkpointSet = new HashSet<>(Arrays.asList(checkpoint1, checkpoint2));

    SystemAdmin mockSystemAdmin = Mockito.mock(SystemAdmin.class);
    // offset 1 < offset2
    Mockito.when(mockSystemAdmin.offsetComparator(offset1, offset2)).thenReturn(-1);
    Mockito.when(mockSystemAdmin.offsetComparator(offset2, offset1)).thenReturn(1);

    // case 1: when exact ssp is in checkpoint set
    Assert.assertEquals(offset2, ElasticityUtils.getMaxOffsetForSSPInCheckpointSet(checkpointSet, ssp, mockSystemAdmin));
    Assert.assertEquals(offset1, ElasticityUtils.getMinOffsetForSSPInCheckpointSet(checkpointSet, ssp, mockSystemAdmin));

    // case 2: when looking for ssp with keyBucket 1 whereas checkpoint set only has full ssp (same system stream and partition but without keybucket)
    SystemStreamPartition sspWithKeyBucket = new SystemStreamPartition(ssp, 1);
    Assert.assertEquals(offset2, ElasticityUtils.getMaxOffsetForSSPInCheckpointSet(checkpointSet, sspWithKeyBucket, mockSystemAdmin));
    Assert.assertEquals(offset1, ElasticityUtils.getMinOffsetForSSPInCheckpointSet(checkpointSet, sspWithKeyBucket, mockSystemAdmin));


    // case 3: when ssp not in checkpoint set -> should receive null for min and max offset
    SystemStreamPartition ssp2 = new SystemStreamPartition("A", "B", new Partition(0));
    Assert.assertEquals(null, ElasticityUtils.getMaxOffsetForSSPInCheckpointSet(checkpointSet, ssp2, mockSystemAdmin));
    Assert.assertEquals(null, ElasticityUtils.getMinOffsetForSSPInCheckpointSet(checkpointSet, ssp2, mockSystemAdmin));
  }

  @Test
  public void testWasElasticityEnabled() {
    Checkpoint checkpoint1 = buildCheckpointV2(new SystemStreamPartition("A", "B", new Partition(0)), "1");
    Checkpoint checkpoint2 = buildCheckpointV2(new SystemStreamPartition("A", "B", new Partition(1)), "2");
    Checkpoint checkpoint3 = buildCheckpointV2(new SystemStreamPartition("A", "B", new Partition(0), 0), "3");
    Checkpoint checkpoint4 = buildCheckpointV2(new SystemStreamPartition("A", "B", new Partition(0), 1), "4");

    // case 0: empty checkpoint map
    Assert.assertFalse(ElasticityUtils.wasElasticityEnabled(new HashMap<>()));

    // case 1: no tasks with elasticity enabled in the checkpoint map
    Map<TaskName, Checkpoint> checkpointMap1 = new HashMap<>();
    checkpointMap1.put(new TaskName("Partition 0"), checkpoint1);
    checkpointMap1.put(new TaskName("Partition 2"), checkpoint2);
    Assert.assertFalse(ElasticityUtils.wasElasticityEnabled(checkpointMap1));

    // case 2: tasks with no elasticity and tasks with elasticity both present in the checkpoint map
    Map<TaskName, Checkpoint> checkpointMap2 = new HashMap<>();
    checkpointMap2.put(new TaskName("Partition 0"), checkpoint1);
    checkpointMap2.put(new TaskName("Partition 2"), checkpoint2);
    checkpointMap2.put(new TaskName("Partition 0_0_2"), checkpoint3);
    Assert.assertTrue(ElasticityUtils.wasElasticityEnabled(checkpointMap2));

    // case 3: only tasks with elasticity present in the checkpoint map
    Map<TaskName, Checkpoint> checkpointMap3 = new HashMap<>();
    checkpointMap3.put(new TaskName("Partition 0_0_2"), checkpoint3);
    checkpointMap3.put(new TaskName("Partition 0_1_2"), checkpoint4);
    Assert.assertTrue(ElasticityUtils.wasElasticityEnabled(checkpointMap3));

    // case 4: repeat same checks with GroupBySSP grouper tasks
    Map<TaskName, Checkpoint> checkpointMap4 = new HashMap<>();
    checkpointMap4.put(new TaskName("SystemStreamPartition [A, B, 0]"), checkpoint1);
    checkpointMap4.put(new TaskName("SystemStreamPartition [A, B, 1]"), checkpoint2);
    Assert.assertFalse(ElasticityUtils.wasElasticityEnabled(checkpointMap4));
    checkpointMap4.put(new TaskName("SystemStreamPartition [A, B, 0, 0]_2"), checkpoint3);
    checkpointMap4.put(new TaskName("SystemStreamPartition [A, B, 0, 1]_2"), checkpoint4);
    Assert.assertTrue(ElasticityUtils.wasElasticityEnabled(checkpointMap4));

    // case 5: repeat same checks with AllSspToSingleTask grouper tasks - no elasticity supported for this grouper
    Map<TaskName, Checkpoint> checkpointMap5 = new HashMap<>();
    checkpointMap5.put(new TaskName("Task-0"), checkpoint1);
    checkpointMap5.put(new TaskName("Task-1"), checkpoint2);
    Assert.assertFalse(ElasticityUtils.wasElasticityEnabled(checkpointMap5));
  }

  private static CheckpointV2 buildCheckpointV2(SystemStreamPartition ssp, String offset) {
    return new CheckpointV2(CheckpointId.create(), ImmutableMap.of(ssp, offset),
        ImmutableMap.of("backend", ImmutableMap.of("store", "10")));
  }
}
