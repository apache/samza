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

package org.apache.samza.operators.impl;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.system.DrainMessage;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;


/**
 * This class tracks the drain state for streams in a task. Internally it keeps track of Drain messages received
 * from upstream tasks for each system stream partition (ssp). If messages have been received from all tasks,
 * it will mark the ssp as drained. For a stream to be drained, all its partitions assigned to
 * the task need to be drained.
 *
 * This class is thread-safe.
 */
public class DrainStates {
  private static final class DrainState {
    // set of upstream tasks
    private final Set<String> tasks = new HashSet<>();
    private final int expectedTotal;
    private volatile boolean drained = false;

    DrainState(int expectedTotal) {
      this.expectedTotal = expectedTotal;
    }

    synchronized void update(String taskName) {
      if (taskName != null) {
        // aggregate the eos messages
        tasks.add(taskName);
        drained = tasks.size() == expectedTotal;
      } else {
        // eos is coming from either source or aggregator task
        drained = true;
      }
    }

    boolean isDrained() {
      return drained;
    }

    @Override
    public String toString() {
      return "DrainState: [Tasks : "
          + tasks
          + ", isDrained : "
          + drained
          + "]";
    }
  }

  private final Map<SystemStreamPartition, DrainState> drainStates;

  /**
   * Constructing the drain states for a task.
   * @param ssps all the ssps assigned to this task
   * @param producerTaskCounts mapping from a stream to the number of upstream tasks that produce to it
   */
  DrainStates(Set<SystemStreamPartition> ssps, Map<SystemStream, Integer> producerTaskCounts) {
    this.drainStates = ssps.stream()
        .collect(Collectors.toMap(
          ssp -> ssp,
          ssp -> new DrainState(producerTaskCounts.getOrDefault(ssp.getSystemStream(), 0))));
  }

  /**
   * Update the state upon receiving a drain message.
   * @param eos message of {@link DrainMessage}
   * @param ssp system stream partition
   */
  void update(DrainMessage eos, SystemStreamPartition ssp) {
    DrainState state = drainStates.get(ssp);
    state.update(eos.getTaskName());
  }

  /**
   * Checks if the system-stream is drained.
   * */
  boolean isDrained(SystemStream systemStream) {
    return drainStates.entrySet().stream()
        .filter(entry -> entry.getKey().getSystemStream().equals(systemStream))
        .allMatch(entry -> entry.getValue().isDrained());
  }

  /**
   * Checks if all streams (input SSPs) for the task has drained.
   * */
  boolean areAllStreamsDrained() {
    return drainStates.values().stream().allMatch(DrainState::isDrained);
  }

  @Override
  public String toString() {
    return drainStates.toString();
  }
}
