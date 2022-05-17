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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.samza.checkpoint.Checkpoint;
import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemAdmins;
import org.apache.samza.system.SystemStreamPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class with util methods to be used for checkpoint computation when elasticity is enabled
 * Elasticity is supported only  for tasks created by either
 * the {@link org.apache.samza.container.grouper.stream.GroupByPartition} SSP grouper or
 * the {@link org.apache.samza.container.grouper.stream.GroupBySystemStreamPartition} SSP grouper
 */
public class ElasticityUtils {
  private static final Logger log = LoggerFactory.getLogger(ElasticityUtils.class);

  // GroupByPartition tasks have names like Partition 0_1_2
  // where 0 is the partition number, 1 is the key bucket and 2 is the elasticity factor
  // see {@link GroupByPartition.ELASTIC_TASK_NAME_FORMAT}
  private static final String ELASTIC_TASK_NAME_GROUP_BY_PARTITION_REGEX = "Partition (\\d+)_(\\d+)_(\\d+)";
  private static final Pattern ELASTIC_TASK_NAME_GROUP_BY_PARTITION_PATTERN = Pattern.compile(ELASTIC_TASK_NAME_GROUP_BY_PARTITION_REGEX);
  private static final String TASK_NAME_GROUP_BY_PARTITION_REGEX = "Partition (\\d+)";
  private static final Pattern TASK_NAME_GROUP_BY_PARTITION_PATTERN = Pattern.compile(TASK_NAME_GROUP_BY_PARTITION_REGEX);
  private static final String TASK_NAME_GROUP_BY_PARTITION_PREFIX = "Partition ";

  //GroupBySSP tasks have names like "SystemStreamPartition [<system>, <Stream>, <partition>, keyBucket]_2"
  // where 2 is the elasticity factor
  // see {@link GroupBySystemStreamPartition} and {@link SystemStreamPartition.toString}
  private static final String ELASTIC_TASK_NAME_GROUP_BY_SSP_REGEX = "SystemStreamPartition \\[(\\S+), (\\S+), (\\d+), (\\d+)\\]_(\\d+)";
  private static final Pattern ELASTIC_TASK_NAME_GROUP_BY_SSP_PATTERN = Pattern.compile(ELASTIC_TASK_NAME_GROUP_BY_SSP_REGEX);
  private static final String TASK_NAME_GROUP_BY_SSP_REGEX = "SystemStreamPartition \\[(\\S+), (\\S+), (\\d+)\\]";
  private static final Pattern TASK_NAME_GROUP_BY_SSP_PATTERN = Pattern.compile(TASK_NAME_GROUP_BY_SSP_REGEX);
  private static final String TASK_NAME_GROUP_BY_SSP_PREFIX = "SystemStreamPartition ";

  /**
   * Elasticity is supported for GroupByPartition tasks and GroupBySystemStreamPartition tasks
   * When elasticity is enabled, GroupByPartition tasks have names Partition 0_1_2
   * When elasticity is enabled, GroupBySystemStreamPartition tasks have names SystemStreamPartition [systemA, streamB, 0, 1]_2
   * Both tasks have names ending with _%d where %d is the elasticity factor
   * @param taskName of either GroupByPartition or GroupBySystemStreamPartition task
   * @return
   *   for GroupByPartition and GroupBySystemStreamPartition tasks returns elasticity factor from the task name
   *   for other tasks returns 1 which is the default elasticity factor
   */
  static int getElasticityFactorFromTaskName(TaskName taskName) {
    return getTaskNameParts(taskName).elasticityFactor;
  }

  /**
   * checks if the given taskname is of a GroupByPartition task
   * @param taskName of any task
   * @return true if GroupByPartition (starts with prefix "Partition ") or false otherwise
   */
  static boolean isGroupByPartitionTask(TaskName taskName) {
    return taskName.getTaskName().startsWith(TASK_NAME_GROUP_BY_PARTITION_PREFIX);
  }

  /**
   * checks if the given taskname is of a GroupBySystemStreamPartition task
   * @param taskName of any task
   * @return true if GroupBySystemStreamPartition (starts with prefix "SystemStreamPartition ") or false otherwise
   */
  static boolean isGroupBySystemStreamPartitionTask(TaskName taskName) {
    return taskName.getTaskName().startsWith(TASK_NAME_GROUP_BY_SSP_PREFIX);
  }

  /**
   * checks if given taskName is elastic aka created with an elasticity factor > 1
   * @param taskName of any task
   * @return true for following, false otherwise
   *    for task created by GroupByPartition, taskName has format "Partition 0_1_2"
   *    for task created by GroupBySystemStreamPartition, taskName has format "SystemStreamPartition [systemA, streamB, 0, 1]_2"
   */
  static boolean isTaskNameElastic(TaskName taskName) {
    if (isGroupByPartitionTask(taskName)) {
      Matcher m = ELASTIC_TASK_NAME_GROUP_BY_PARTITION_PATTERN.matcher(taskName.getTaskName());
      return m.find();
    } else if (isGroupBySystemStreamPartitionTask(taskName)) {
      Matcher m = ELASTIC_TASK_NAME_GROUP_BY_SSP_PATTERN.matcher(taskName.getTaskName());
      return m.find();
    }
    return false;
  }

  /**
   * From given taskName extract the values for system, stream, partition, keyBucket and elasticityFactor
   * @param taskName any taskName
   * @return TaskNameComponents object containing system, stream, partition, keyBucket and elasticityFactor
   *    for GroupByPartition task:
   *         taskNames are of the format "Partition 0_1_2" (with elasticity) or "Partition 0" (without elasticity)
   *         system and stream are empty "" strings and partition is the input partition,
   *         without elasticity, keyBucket = 0 and elasticityFactor = 1 (the default values)
   *         with elasticity, keyBucket from name (ex 1 above) and elasticityFactor (ex 2 from above)
   *    for GroupBySystemStreamPartition task:
   *         taskNames are of the format "SystemStreamPartition [systemA, streamB, 0, 1]_2" (with elasticity) or
   *         "SystemStreamPartition [systemA, streamB, 0]" (without elasticity)
   *         system and stream and partition are from the name (ex system = systemA, steram = streamB, partition =0 above)
   *         without elasticity, keyBucket = 0 and elasticityFactor = 1 (the default values)
   *         with elasticity, keyBucket from name (ex 1 above) and elasticityFactor (ex 2 from above)
   *   for tasks created with other SSP groupers:
   *        default TaskNameComponents is returned which has empty system, stream,
   *        -1 for partition and 0 for keyBucket and 1 for elasticity factor
   */
  static TaskNameComponents getTaskNameParts(TaskName taskName) {
    if (isGroupByPartitionTask(taskName)) {
      return getTaskNameParts_GroupByPartition(taskName);
    } else if (isGroupBySystemStreamPartitionTask(taskName)) {
      return getTaskNameParts_GroupBySSP(taskName);
    }
    log.warn("TaskName {} is neither GroupByPartition nor GroupBySystemStreamPartition task. "
        + "Elasticity is not supported for this taskName. "
        + "Returning default TaskNameComponents which has default keyBucket 0,"
        + " default elasticityFactor 1 and invalid partition -1", taskName.getTaskName());
    return new TaskNameComponents(TaskNameComponents.INVALID_PARTITION);
  }

  /**
   * see doc for getTaskNameParts above
   */
  private static TaskNameComponents getTaskNameParts_GroupByPartition(TaskName taskName) {
    String taskNameStr = taskName.getTaskName();
    log.debug("GetTaskNameParts for taskName {}", taskNameStr);

    Matcher matcher = ELASTIC_TASK_NAME_GROUP_BY_PARTITION_PATTERN.matcher(taskNameStr);
    if (matcher.find()) {
      return new TaskNameComponents(Integer.valueOf(matcher.group(1)),
          Integer.valueOf(matcher.group(2)),
          Integer.valueOf(matcher.group(3)));
    }
    matcher = TASK_NAME_GROUP_BY_PARTITION_PATTERN.matcher(taskNameStr);
    if (matcher.find()) {
      return new TaskNameComponents(Integer.valueOf(matcher.group(1)));
    }
    log.error("Could not extract partition, keybucket and elasticity factor from taskname for task {}.", taskNameStr);
    throw new IllegalArgumentException("TaskName format incompatible");
  }

  /**
   * see doc for getTaskNameParts above
   */
  private static TaskNameComponents getTaskNameParts_GroupBySSP(TaskName taskName) {
    String taskNameStr = taskName.getTaskName();
    log.debug("GetTaskNameParts for taskName {}", taskNameStr);

    Matcher matcher = ELASTIC_TASK_NAME_GROUP_BY_SSP_PATTERN.matcher(taskNameStr);
    if (matcher.find()) {
      return new TaskNameComponents(matcher.group(1),
          matcher.group(2),
          Integer.valueOf(matcher.group(3)),
          Integer.valueOf(matcher.group(4)),
          Integer.valueOf(matcher.group(5)));
    }
    matcher = TASK_NAME_GROUP_BY_SSP_PATTERN.matcher(taskNameStr);
    if (matcher.find()) {
      return new TaskNameComponents(matcher.group(1),
          matcher.group(2),
          Integer.valueOf(matcher.group(3)));
    }
    log.warn("Could not extract system, stream, partition, keybucket and elasticity factor from taskname for task {}.", taskNameStr);
    throw new IllegalArgumentException("TaskName format incompatible");
  }

  /**
   * Without elasticity, a task consumes an entire (full) SSP = [System, stream, partition].
   * With elasticity, a task consumes a portion of the SSP_withKeyBucket = [system, stream, partition, keyBucket]
   *    where 0 <= keyBucket < elasticityFactor and contains a subset of the IncomingMessageEnvelope(IME) from the full SSP
   * Given two tasks currentTask and otherTask, the task otherTask is called ancestor of currentTask if the following is true
   *    all IME consumed by currentTask will be consumed by otherTask when elasticityFactor decreases or stays same
   *    For example:
   *      case 1: elasticityFactor 2 to 1
   *            otherTask = Partition 0 consuming all IME in SSP = [systemA, streamB, 0] when elasticityFactor=1
   *            currentTask1 = Partition 0_0_2 consumes IME in SSP_withKeyBucket0 = [systemA, streamB, 0, 0 (keyBucket)] when elasticityFactor = 2
   *            currentTask2 = Partition 0_1_2 consumes IME in SSP_withKeyBucket1 = [systemA, streamB, 0, 1 (keyBucket)] when elasticityFactor = 2
   *            SSP =  SSP_withKeyBucket0 + SSP_withKeyBucket1. Thus, Partition 0 is ancestor of Partition 0_0_2 and Partition 0_1_2
   *      case 2: elasticityFactor 2 to 2 - no change
   *            Partition 0_0_2 is an ancestor of itself since the input SSP_withKeyBucket0 doesnt change
   *            similarly Partition 0_1_2 is an ancestor of itself. This applies to all elasticityFactors
   *      case 3: elasticityFactor 4 to 2
   *            otherTask = Partition 0_0_2 consuming all IME in SSP_withKeyBucket0 = [systemA, streamB, 0, 0] when elasticityFactor=2
   *            currentTask1 = Partition 0_0_4 consumes IME in SSP_withKeyBucket00 = [systemA, streamB, 0, 0 (keyBucket)] when elasticityFactor = 4
   *            currentTask2 = Partition 0_2_4 consumes IME in SSP_withKeyBucket01 = [systemA, streamB, 0, 2 (keyBucket)] when elasticityFactor = 4
   *            From the computation of SSP_withkeyBucket in {@link org.apache.samza.system.IncomingMessageEnvelope}
   *            we have getSystemStreamPartition(int elasticityFactor) which does keyBucket = (Math.abs(envelopeKeyorOffset.hashCode()) % 31) % elasticityFactor;
   *            Thus, SSP_withKeyBucket0 = SSP_withKeyBucket00 + SSP_withKeyBucket01.
   *            Thus, Partition 0_0_2 is ancestor of Partition 0_0_4 and Partition 0_2_4
   *            Similarly, Partition 0_1_2 is ancestor of Partition 0_1_4 and Partition 0_3_4
   *            And transitively, Partition 0 is ancestor of Partition 0_0_4, Partition 0_1_4, Partition 0_2_4 and Partition 0_3_4
   *
   * This applies to tasks created by GroupByPartition and GroupBySystemStreamPartition SSPGroupers.
   * aka this applies if both currentTask and otherTask are created by GroupByPartition or both are created by GroupBySystemStreamPartition
   * If either currentTask and/or otherTask were created by other SSPGroupers then false is returned.
   * @param currentTask
   * @param otherTask
   * @return true if otherTask is ancestor of currentTask, false otherwise
   */
  static boolean isOtherTaskAncestorOfCurrentTask(TaskName currentTask, TaskName otherTask) {
    log.debug("isOtherTaskAncestorOfCurrentTask with currentTask {} and otherTask {}", currentTask, otherTask);
    if (!((isGroupByPartitionTask(currentTask) && isGroupByPartitionTask(otherTask))
        || (isGroupBySystemStreamPartitionTask(currentTask) && isGroupBySystemStreamPartitionTask(otherTask)))) {
      return false;
    }

    TaskNameComponents currentTaskNameComponents = getTaskNameParts(currentTask);
    TaskNameComponents otherTaskNameComponents = getTaskNameParts(otherTask);

    if (!otherTaskNameComponents.system.equals(currentTaskNameComponents.system)
        || !otherTaskNameComponents.stream.equals(currentTaskNameComponents.stream)
        || otherTaskNameComponents.partition != currentTaskNameComponents.partition
        || otherTaskNameComponents.elasticityFactor > currentTaskNameComponents.elasticityFactor) {
      return false;
    }

    return (currentTaskNameComponents.keyBucket % otherTaskNameComponents.elasticityFactor) == otherTaskNameComponents.keyBucket;
  }

  /**
   * See javadoc for isOtherTaskAncestorOfCurrentTask above
   * Given currentTask and otherTask,
   *   if currentTask == otherTask, then its not a descendant. (unlike ancestor)
   *   else, if isOtherTaskAncestorOfCurrentTask(otherTask, currentTask) then otherTask is descendant of currentTask
   * @param currentTask
   * @param otherTask
   * @return
   */
  static boolean isOtherTaskDescendantOfCurrentTask(TaskName currentTask, TaskName otherTask) {
    log.debug("isOtherTaskDescendantOfCurrentTask with currentTask {} and otherTask {}", currentTask, otherTask);
    if (!((isGroupByPartitionTask(currentTask) && isGroupByPartitionTask(otherTask))
        || (isGroupBySystemStreamPartitionTask(currentTask) && isGroupBySystemStreamPartitionTask(otherTask)))) {
      return false;
    }

    TaskNameComponents currentTaskNameComponents = getTaskNameParts(currentTask);
    TaskNameComponents otherTaskNameComponents = getTaskNameParts(otherTask);

    if (!otherTaskNameComponents.system.equals(currentTaskNameComponents.system)
        || !otherTaskNameComponents.stream.equals(currentTaskNameComponents.stream)
        || otherTaskNameComponents.partition != currentTaskNameComponents.partition
        || otherTaskNameComponents.elasticityFactor <= currentTaskNameComponents.elasticityFactor) {
      return false;
    }

    return (
        otherTaskNameComponents.keyBucket % currentTaskNameComponents.elasticityFactor) == currentTaskNameComponents.keyBucket;
  }

  /**
   * For a given taskName and a map of task names to checkpoints, returns the taskName's ancestor and descendants checkpoints
   * All ancestor checkpoints are put into a set
   * Descendant checkpoins are put into a map of elasticityFactor to descendant checkpoint where the elastictyFactor is of the descendant.
   * For example, given taskName Partition 0_0_2 and checkpoint Map (Partition 0->C1, Partition 0_0_4-> C2, Partition 0_1_4 -> C3, Partition 0_2_4 ->C4)
   * the return value is AncestorSet = <C1> and descendantMap = (4 -> <C2, C4>)
   * See javadoc of isOtherTaskAncestorOfCurrentTask and isOtherTaskDescendantOfCurrentTask for definition of ancestor and descendant
   * @param taskName name of the task
   * @param checkpointMap map from taskName to checkpoint
   * @return Pair of AncestorCheckpoint set and Descendant Checkpoint Map
   */
  static Pair<Set<Checkpoint>, Map<Integer, Set<Checkpoint>>> getAncestorAndDescendantCheckpoints(
      TaskName taskName, Map<TaskName, Checkpoint> checkpointMap) {
    Set<Checkpoint> ancestorCheckpoints = new HashSet<>();
    Map<Integer, Set<Checkpoint>> descendantCheckpoints = new HashMap<>();
    log.debug("starting to parse the checkpoint map to find ancestors and descendants for taskName {}", taskName.getTaskName());
    checkpointMap.keySet().forEach(otherTaskName -> {
      Checkpoint otherTaskCheckpoint = checkpointMap.get(otherTaskName);
      if (isOtherTaskAncestorOfCurrentTask(taskName, otherTaskName)) {
        log.debug("current task name is {} and other task name is {} and other task is ancestor", taskName, otherTaskName);
        ancestorCheckpoints.add(otherTaskCheckpoint);
      }
      if (isOtherTaskDescendantOfCurrentTask(taskName, otherTaskName)) {
        log.debug("current task name is {} and other task name is {} and other task is descendant", taskName, otherTaskName);
        int otherEF = getElasticityFactorFromTaskName(otherTaskName);
        if (!descendantCheckpoints.containsKey(otherEF)) {
          descendantCheckpoints.put(otherEF, new HashSet<>());
        }
        descendantCheckpoints.get(otherEF).add(otherTaskCheckpoint);
      }
    });
    log.debug("done computing all ancestors and descendants of {}", taskName);
    return new ImmutablePair<>(ancestorCheckpoints, descendantCheckpoints);
  }

  /**
   * Given a checkpoint with offset map from SystemStreamPartition to offset, returns the offset for the desired ssp
   * Only the system, stream and partition portions of the SSP are matched, the keyBucket is not considered.
   * A checkpoint belongs to one task and a task would consume either the full SSP (aka no keyBucket)
   * or consume exactly one of the keyBuckets of an SSP. Hence there will be at most one entry for an SSP in a checkpoint
   * @param checkpoint Checkpoint containing SSP -> offset
   * @param ssp SystemStreamPartition for which an offset needs to be fetched
   * @return offset for the ssp in the Checkpoint or null if doesnt exist.
   */
  static String getOffsetForSSPInCheckpoint(Checkpoint checkpoint, SystemStreamPartition ssp) {
    String checkpointStr = checkpoint.getOffsets().entrySet().stream()
        .map(k -> k.getKey() + " : " + k.getValue())
        .collect(Collectors.joining(", ", "{", "}"));
    log.debug("for ssp {}, in checkpoint {}", ssp, checkpointStr);

    Optional<String> offsetFound = checkpoint.getOffsets().entrySet()
        .stream()
        .filter(entry -> entry.getKey().getSystemStream().equals(ssp.getSystemStream()) && entry.getKey()
            .getPartition()
            .equals(ssp.getPartition()))
        .map(Map.Entry::getValue)
        .findFirst();
    if (offsetFound.isPresent()) {
      return offsetFound.get();
    }
    log.warn("Could not find offset for ssp {} in checkpoint {}. returning null string as offset", ssp, checkpoint);
    return null;
  }

  /**
   * Given a set of checkpoints, find the max aka largest offset for an ssp
   * Largest is determined by the SystemAdmin.offsetCompartor of the ssp's system.
   * Only the system, stream and partition portions of the SSP are matched, the keyBucket is not considered.
   * @param checkpointSet set of checkpoints
   * @param ssp for which largest offset is needed
   * @param systemAdmin of the ssp.getSystem()
   * @return offset - string if one exists else null
   */
  static String getMaxOffsetForSSPInCheckpointSet(Set<Checkpoint> checkpointSet,
      SystemStreamPartition ssp, SystemAdmin systemAdmin) {
    return checkpointSet.stream()
        .filter(Objects::nonNull)
        .map(checkpoint -> getOffsetForSSPInCheckpoint(checkpoint, ssp))
        .filter(Objects::nonNull)
        .sorted((offset1, offset2) -> systemAdmin.offsetComparator(offset2, offset1)) //confirm reverse sort - aka largest offset first
        .findFirst().orElse(null);
  }

  /**
   * Given a set of checkpoints, find the min aka smallest offset for an ssp
   * Smallest is determined by the SystemAdmin.offsetCompartor of the ssp's system.
   * Only the system, stream and partition portions of the SSP are matched, the keyBucket is not considered.
   * @param checkpointSet set of checkpoints
   * @param ssp for which largest offset is needed
   * @param systemAdmin of the ssp.getSystem()
   * @return offset - string if one exists else null
   */
  static String getMinOffsetForSSPInCheckpointSet(Set<Checkpoint> checkpointSet,
      SystemStreamPartition ssp, SystemAdmin systemAdmin) {
    return checkpointSet.stream()
        .filter(Objects::nonNull)
        .map(checkpoint -> getOffsetForSSPInCheckpoint(checkpoint, ssp))
        .filter(Objects::nonNull)
        .sorted((offset1, offset2) -> systemAdmin.offsetComparator(offset1, offset2)) //confirm ascending sort - aka smallest offset first
        .findFirst().orElse(null);
  }

  /**
   * Prereq: See javadoc for isOtherTaskAncestorOfCurrentTask and isOtherTaskDescendantOfCurrentTask to fully understand ancestor and descendant notion
   * Briefly, Given tasks - Partition 0, Partition 0_0_2, Partition 0_1_2 and Partition 0_0_4, Partition 0_1_4, Partition 0_2_4 and Partition 0_3_4
   * (recall Partition 0_1_2 means reads input partition 0, keyBucket 1 and elasticityFactor 2)
   * For task Partition 0_0_2: ancestors = [Partition 0, Partition 0_0_2] and descendants = [Partition 0_0_4, Partition 0_2_4]
   *
   * If a task has no descendants, then we just need to pick the largest offset among all the ancestors to get the last processed offset.
   * for example above, if Partition 0_0_2 only had ancestors and no descendants, taking largest offset among Partition 0 and 0_0_2 gives last proc offset.
   *
   * With descendants, a little care is needed. there could be descendants with different elasticity factors.
   * given one elasticity factor, each the descendant within the elasticity factor consumes a sub-portion (aka keyBucket) of the task.
   * hence, to avoid data loss, we need to pick the lowest offset across descendants of the same elasticity factor.
   * Across elasticity factors, largest works just like in ancestor
   *
   * Taking a concrete example
   * From {@link org.apache.samza.system.IncomingMessageEnvelope} (IME)
   *    Partition 0 consunmig all IME in SSP = [systemA, streamB, 0] when elasticityFactor=1
   *    Partition 0_1_2 consuming all IME in SSP_withKeyBucket0 = [systemA, streamB, 0, 1 (keyBucket)] when elasticityFactor=2
   *    Partition 0_0_2 consuming all IME in SSP_withKeyBucket1 = [systemA, streamB, 0, 0 (keyBucket)] when elasticityFactor=2
   *    Partition 0_0_4 consumes IME in SSP_withKeyBucket00 = [systemA, streamB, 0, 0 (keyBucket)] when elasticityFactor = 4
   *    Partition 0_2_4 consumes IME in SSP_withKeyBucket01 = [systemA, streamB, 0, 2 (keyBucket)] when elasticityFactor = 4
   *    From the computation of SSP_withkeyBucket in {@link org.apache.samza.system.IncomingMessageEnvelope}
   *    we have getSystemStreamPartition(int elasticityFactor) which does keyBucket = (Math.abs(envelopeKeyorOffset.hashCode()) % 31) % elasticityFactor;
   *    Thus,
   *       SSP = SSP_withKeyBucket0 + SSP_withKeyBucket1.
   *       SSP_withKeyBucket0 = SSP_withKeyBucket00 + SSP_withKeyBucket01.
   *    If the checkpoint map has
   *      Partition 0: (SSP : 1), Partition 0_0_2: (SSP0 : 2), Partition 0_1_2: (SSP1 : 3), Partition 0_0_4: (SSP0 : 4), Partition 0_2_4: (SSP1 : 6)
   *      looking at these map and knowing that offsets are monotonically increasing, it is clear that last deploy was with elasticity factor = 4
   *      to get checkpoint for Partition 0_0_2, we need to consider last deploy's offsets.
   *      picking 6 (offset for Partition 0_2_4) means that 0_0_2 will start proc from 6 but offset 5 was never processed.
   *      hence we need to take min of offsets within an elasticity factor.
   *
   * Given checkpoints for all the tasks in the checkpoint stream,
   * computing the last proc offset for an ssp checkpoint for a task,
   * the following needs to be met.
   *    1. Ancestors: we need to take largest offset among ancestors for an ssp
   *    2. Descendants:
   *         a. group descendants by their elasticityFactor.
   *         b. among descendants of the same elasticityFactor, take the smallest offset for an ssp
   *         c. once step b is done, we have (elasticityFactor : smallest-offset-for-ssp) set, pick the largest in this set
   *    3. Pick the larger among the offsets received from step 1 (for ancestors) and step 2 (for descendants)
   *
   * @param taskName
   * @param taskSSPSet
   * @param checkpointMap
   * @param systemAdmins
   * @return
   */
  public static Map<SystemStreamPartition, String> computeLastProcessedOffsetsFromCheckpointMap(
      TaskName taskName,
      Set<SystemStreamPartition> taskSSPSet,
      Map<TaskName, Checkpoint> checkpointMap,
      SystemAdmins systemAdmins) {
    Pair<Set<Checkpoint>, Map<Integer, Set<Checkpoint>>> acnestorsAndDescendantsFound =
        getAncestorAndDescendantCheckpoints(taskName, checkpointMap);
    Set<Checkpoint> ancestorCheckpoints = acnestorsAndDescendantsFound.getLeft();
    Map<Integer, Set<Checkpoint>> descendantCheckpoints = acnestorsAndDescendantsFound.getRight();

    Map<SystemStreamPartition, String> taskSSPOffsets = new HashMap<>();

    taskSSPSet.forEach(ssp_withKeyBucket -> {
      log.info("for taskName {} and ssp of the task {}, finding its last proc offset", taskName, ssp_withKeyBucket);

      SystemStreamPartition ssp = new SystemStreamPartition(ssp_withKeyBucket.getSystemStream(),
          ssp_withKeyBucket.getPartition());

      SystemAdmin systemAdmin = systemAdmins.getSystemAdmin(ssp.getSystem());

      String currentLastOffsetForSSP = null;

      String ancestorLastOffsetForSSP = getMaxOffsetForSSPInCheckpointSet(ancestorCheckpoints, ssp, systemAdmin);

      log.info("for taskName {} and ssp {} got lastoffset from ancestors as {}",
          taskName, ssp_withKeyBucket, ancestorLastOffsetForSSP);

      String descendantLastOffsetForSSP = descendantCheckpoints.entrySet().stream()
          .map(entry -> getMinOffsetForSSPInCheckpointSet(entry.getValue(), ssp, systemAdmin)) // at each ef level, find min offset
          .sorted((offset1, offset2) -> systemAdmin.offsetComparator(offset2, offset1)) //confirm reverse sort - aka largest offset first
          .filter(Objects::nonNull)
          .findFirst().orElse(null);

      log.info("for taskName {} and ssp {} got lastoffset from descendants as {}",
          taskName, ssp_withKeyBucket, descendantLastOffsetForSSP);

      Integer offsetComparison = systemAdmin.offsetComparator(ancestorLastOffsetForSSP, descendantLastOffsetForSSP);
      if (descendantLastOffsetForSSP == null || (offsetComparison != null && offsetComparison > 0)) { // means ancestorLastOffsetForSSP > descendantLastOffsetForSSP
        currentLastOffsetForSSP = ancestorLastOffsetForSSP;
      } else {
        currentLastOffsetForSSP = descendantLastOffsetForSSP;
      }
      if (currentLastOffsetForSSP == null) {
        log.info("for taskName {} and ssp {} got lastoffset as null. "
            + "skipping adding this ssp to task's offsets loaded from previous checkpoint", taskName, ssp_withKeyBucket);
      } else {
        log.info("for taskName {} and ssp {} got lastoffset as {}", taskName, ssp_withKeyBucket, currentLastOffsetForSSP);
        taskSSPOffsets.put(ssp_withKeyBucket, currentLastOffsetForSSP);
      }
    });

    String checkpointStr = taskSSPOffsets.entrySet().stream()
        .map(k -> k.getKey() + " : " + k.getValue())
        .collect(Collectors.joining(", ", "{", "}"));
    log.info("for taskName {}, returning checkpoint as {}", taskName, checkpointStr);
    return taskSSPOffsets;
  }

  public static boolean wasElasticityEnabled(Map<TaskName, Checkpoint> checkpointMap) {
    return checkpointMap.keySet().stream()
        .filter(ElasticityUtils::isTaskNameElastic) // true if the taskName has elasticityFactor in it
        .findFirst().isPresent();
  }
}
