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

package org.apache.samza.control;

import com.google.common.collect.Multimap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.system.EndOfStreamMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class handles the end-of-stream control message. It aggregates the end-of-stream state for each input ssps of
 * a task, and propagate the eos messages to downstream intermediate streams if needed.
 *
 * Internal use only.
 */
public class EndOfStreamManager {
  private static final Logger log = LoggerFactory.getLogger(EndOfStreamManager.class);

  private final String taskName;
  private final MessageCollector collector;
  // end-of-stream state per ssp
  private final Map<SystemStreamPartition, EndOfStreamState> eosStates;
  private final StreamMetadataCache metadataCache;
  // mapping from output stream to its upstream task count
  private final Map<SystemStream, Integer> upstreamTaskCounts;
  private final IOGraph ioGraph = null;

  public EndOfStreamManager(String taskName,
      Multimap<SystemStream, String> inputToTasks,
      Set<SystemStreamPartition> ssps,
      StreamMetadataCache metadataCache,
      MessageCollector collector) {
    this.taskName = taskName;
    this.metadataCache = metadataCache;
    this.collector = collector;
    Map<SystemStreamPartition, EndOfStreamState> states = new HashMap<>();
    ssps.forEach(ssp -> {
        states.put(ssp, new EndOfStreamState());
      });
    this.eosStates = Collections.unmodifiableMap(states);
    this.upstreamTaskCounts = ControlMessageUtils.calculateUpstreamTaskCounts(inputToTasks, ioGraph);
  }

  public void update(IncomingMessageEnvelope envelope, TaskCoordinator coordinator) {
    EndOfStreamState state = eosStates.get(envelope.getSystemStreamPartition());
    EndOfStreamMessage message = (EndOfStreamMessage) envelope.getMessage();
    state.update(message.getTaskName(), 0);
    log.info("Received end-of-stream from task " + message.getTaskName() + " in " + envelope.getSystemStreamPartition());

    SystemStream systemStream = envelope.getSystemStreamPartition().getSystemStream();
    if (isEndOfStream(systemStream)) {
      log.info("End-of-stream of input " + systemStream + " for " + systemStream);
      ioGraph.getNodesOfInput(systemStream).forEach(node -> {
          // find the intermediate streams that need broadcast the eos messages
          if (node.isOutputIntermediate()) {
            // check all the input stream partitions assigned to the task are end-of-stream
            boolean inputsEndOfStream = node.getInputs().stream().allMatch(spec -> isEndOfStream(spec.toSystemStream()));
            if (inputsEndOfStream) {
              // broadcast the end-of-stream message to the intermediate stream
              SystemStream outputStream = node.getOutput().toSystemStream();
              sendEndOfStream(outputStream, upstreamTaskCounts.get(outputStream));
            }
          }
        });

      boolean allEndOfStream = eosStates.values().stream().allMatch(EndOfStreamState::isEndOfStream);
      if (allEndOfStream) {
        // all inputs have been end-of-stream, shut down the task
        log.info("All input streams have reached the end for task " + taskName);
        coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
      }
    }
  }

  /**
   * Return true if all partitions of the systemStream that are assigned to the current task have reached EndOfStream.
   * @param systemStream stream
   * @return whether the stream reaches to the end for this task
   */
  boolean isEndOfStream(SystemStream systemStream) {
    return eosStates.entrySet().stream()
        .filter(entry -> entry.getKey().getSystemStream().equals(systemStream))
        .allMatch(entry -> entry.getValue().isEndOfStream());
  }

  /**
   * Send the EndOfStream control messages to downstream
   * @param systemStream downstream stream
   */
  void sendEndOfStream(SystemStream systemStream, int taskCount) {
    log.info("Send end-of-stream messages with upstream task count {} to all partitions of {}", taskCount, systemStream);
    final EndOfStreamMessage message = new EndOfStreamMessage(taskName);
    ControlMessageUtils.sendControlMessage(message, systemStream, metadataCache, collector);
  }

  /**
   * This class keeps the internal state for a ssp to be end-of-stream.
   */
  final static class EndOfStreamState {
    // set of upstream tasks
    private final Set<String> tasks = new HashSet<>();
    private int expectedTotal = Integer.MAX_VALUE;
    private boolean isEndOfStream = false;

    void update(String taskName, int taskCount) {
      if (taskName != null) {
        tasks.add(taskName);
      }
      expectedTotal = taskCount;
      isEndOfStream = tasks.size() == expectedTotal;
    }

    boolean isEndOfStream() {
      return isEndOfStream;
    }
  }

}
