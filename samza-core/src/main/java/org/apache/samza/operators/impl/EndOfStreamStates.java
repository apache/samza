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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;


/**
 * This class manages the end-of-stream state of the streams in a task. Internally it keeps track of end-of-stream
 * messages received from upstream tasks for each system stream partition (ssp). If messages have been received from
 * all tasks, it will mark the ssp as end-of-stream. For a stream to be end-of-stream, all its partitions assigned to
 * the task need to be end-of-stream.
 *
 * This class is thread-safe.
 */
class EndOfStreamStates {

  private static final class EndOfStreamState {
    // set of upstream tasks
    private final Set<String> tasks = new HashSet<>();
    private final int expectedTotal;
    private volatile boolean isEndOfStream = false;

    EndOfStreamState(int expectedTotal) {
      this.expectedTotal = expectedTotal;
    }

    synchronized void update(String taskName) {
      if (taskName != null) {
        tasks.add(taskName);
      }
      isEndOfStream = tasks.size() == expectedTotal;
    }

    boolean isEndOfStream() {
      return isEndOfStream;
    }
  }

  private final Map<SystemStreamPartition, EndOfStreamState> eosStates;

  /**
   * Constructing the end-of-stream states for a task
   * @param ssps all the ssps assigned to this task
   * @param producerTaskCounts mapping from a stream to the number of upstream tasks that produce to it
   */
  EndOfStreamStates(Set<SystemStreamPartition> ssps, Map<SystemStream, Integer> producerTaskCounts) {
    Map<SystemStreamPartition, EndOfStreamState> states = new HashMap<>();
    ssps.forEach(ssp -> {
        states.put(ssp, new EndOfStreamState(producerTaskCounts.getOrDefault(ssp.getSystemStream(), 0)));
      });
    this.eosStates = Collections.unmodifiableMap(states);
  }

  /**
   * Update the state upon receiving an end-of-stream message.
   * @param eos message of {@link EndOfStreamMessage}
   * @param ssp system stream partition
   */
  void update(EndOfStreamMessage eos, SystemStreamPartition ssp) {
    EndOfStreamState state = eosStates.get(ssp);
    state.update(eos.getTaskName());
  }

  boolean isEndOfStream(SystemStream systemStream) {
    return eosStates.entrySet().stream()
        .filter(entry -> entry.getKey().getSystemStream().equals(systemStream))
        .allMatch(entry -> entry.getValue().isEndOfStream());
  }

  boolean allEndOfStream() {
    return eosStates.values().stream().allMatch(EndOfStreamState::isEndOfStream);
  }
}