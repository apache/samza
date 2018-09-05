package org.apache.samza.container.grouper.stream;

import org.apache.samza.container.TaskName;
import org.apache.samza.system.SystemStreamPartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;


/**
 * Helper class for Grouper
 */
public class GroupingHelper {

  /**
   * Compute the map of Stream name to map of PartitionId to {@link SystemStreamPartition}
   *
   * @param   ssps Set of {@link SystemStreamPartition}
   * @return map(streamName -> map(PartitionId -> Ssp)
   */
  public static Map<String, Map<Integer, SystemStreamPartition>> getStreamToPartitionSspMap(
      Optional<Set<SystemStreamPartition>> ssps) {
    Map<String, Map<Integer, SystemStreamPartition>> streamToPartitionSspMap = new HashMap<>();
    if (ssps.isPresent()) {
      ssps.get().forEach((ssp) -> {
        if (streamToPartitionSspMap.get(ssp.getStream()) == null) {
          Map<Integer, SystemStreamPartition> partitionSspMap = new HashMap<>();
          partitionSspMap.put(ssp.getPartition().getPartitionId(), ssp);
          streamToPartitionSspMap.put(ssp.getStream(), partitionSspMap);
        } else {
          streamToPartitionSspMap.get(ssp.getStream()).put(ssp.getPartition().getPartitionId(), ssp);
        }
      });
    }

    return streamToPartitionSspMap;
  }

  /**
   * Returns new grouping of previous {@link SystemStreamPartition} taking in to consideration repartitioning, only if the current set of {@link SystemStreamPartition} contain them
   *
   * @param   currSsps
   *          Current set of {@link SystemStreamPartition}
   *
   * @param   previousGrouping
   *          Previous grouping that was available.
   *
   * @return  {@link RepartioningHelper}
   */

  public static RepartioningHelper getGroupingForRepartition(Optional<Set<SystemStreamPartition>> currSsps,
      Optional<Map<TaskName, Set<SystemStreamPartition>>> previousGrouping) {
    final Map<TaskName, Set<SystemStreamPartition>> finalGrouping = new HashMap<>();

    Map<SystemStreamPartition, TaskName> prevSspToTaskMap = new HashMap<>();
    Set<SystemStreamPartition> prevSsps = new HashSet<>();
    if (previousGrouping.isPresent()) {
      previousGrouping.get().forEach((k, v) -> {
        prevSsps.addAll(v);
        v.forEach(ssp -> prevSspToTaskMap.put(ssp, k));
      });
    }

    Map<String, Map<Integer, SystemStreamPartition>> currStreamToSspPartitionMap = getStreamToPartitionSspMap(currSsps);
    Map<String, Map<Integer, SystemStreamPartition>> prevStreamToSspPartitionMap =
        getStreamToPartitionSspMap(Optional.of(prevSsps));

    Set<SystemStreamPartition> newSsps = new HashSet<>();

    currStreamToSspPartitionMap.forEach((stream, currPartitionToSspMap) -> {
      if (prevStreamToSspPartitionMap.containsKey(stream)) {
        int currPartitionCount = currPartitionToSspMap.size();
        int prevPartitionCount = prevStreamToSspPartitionMap.get(stream).size();
        if (currPartitionCount % prevPartitionCount == 0) {
          int expansionFactor = currPartitionCount / prevPartitionCount;
          for (int i = expansionFactor - 1; i >= 0; i--) {
            Map<Integer, SystemStreamPartition> prevPartitionToSsp = prevStreamToSspPartitionMap.get(stream);
            int multiplier = i;
            prevPartitionToSsp.forEach((prevPartitionId, ssp) -> {
              // get the task for the associated StreamSystemPartition
              TaskName taskName = prevSspToTaskMap.get(ssp);
              // compute the newly assigned partition
              int assignedPartitionId = prevPartitionId + (multiplier * prevPartitionCount);
              // get the newly assigned SSP from the partitionId
              SystemStreamPartition assignedSsp = currPartitionToSspMap.get(assignedPartitionId);
              // update the final grouping
              if (finalGrouping.get(taskName) == null) {
                Set<SystemStreamPartition> finalSspSet = new HashSet<>();
                finalSspSet.add(assignedSsp);
                finalGrouping.put(taskName, finalSspSet);
              } else {
                finalGrouping.get(taskName).add(assignedSsp);
              }
            });
          }
        }
      } else {
        // separate out newly added streams to group them once the existng streams have been handled.
        newSsps.addAll(currPartitionToSspMap.values());
      }
    });

    return new RepartioningHelper(finalGrouping, newSsps);
  }

  /**
   * Wrapper for the new grouping for the existing {@link SystemStreamPartition} and set of {@link SystemStreamPartition} for new Streams
   */
  public static final class RepartioningHelper {
    private final Map<TaskName, Set<SystemStreamPartition>> grouping;
    private final Set<SystemStreamPartition> newSsps;

    public RepartioningHelper(Map<TaskName, Set<SystemStreamPartition>> grouping, Set<SystemStreamPartition> newSsps) {
      this.grouping = grouping;
      this.newSsps = newSsps;
    }

    public Map<TaskName, Set<SystemStreamPartition>> getGrouping() {
      return grouping;
    }

    public Set<SystemStreamPartition> getNewSsps() {
      return newSsps;
    }
  }
}
