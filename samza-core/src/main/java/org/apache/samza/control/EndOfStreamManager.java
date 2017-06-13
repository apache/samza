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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.samza.control.ControlMessageManager.ControlManager;
import org.apache.samza.message.EndOfStreamMessage;
import org.apache.samza.operators.StreamGraphImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.operators.util.IOGraphUtil.IONode;
import org.apache.samza.system.EndOfStream;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class handles the end-of-stream control message. It aggregates the end-of-stream state for each input ssps of
 * a task, and propagate the eos messages to downstream intermediate streams if needed.
 */
public class EndOfStreamManager implements ControlManager {
  private static final Logger log = LoggerFactory.getLogger(EndOfStreamManager.class);
  private static final String EOS_KEY_FORMAT = "%s-%s-EOS"; //stream-task-EOS

  private final String taskName;
  private final int taskCount;
  private final MessageCollector collector;
  private final Map<SystemStreamPartition, EndOfStreamState> inputStates;
  private final Map<String, SystemAdmin> sysAdmins;

  public EndOfStreamManager(String taskName,
      int taskCount,
      Set<SystemStreamPartition> ssps,
      Map<String, SystemAdmin> sysAdmins, MessageCollector collector) {
    this.taskName = taskName;
    this.taskCount = taskCount;
    this.sysAdmins = sysAdmins;
    this.collector = collector;
    Map<SystemStreamPartition, EndOfStreamState> states = new HashMap<>();
    ssps.forEach(ssp -> {
        states.put(ssp, new EndOfStreamState());
      });
    this.inputStates = Collections.unmodifiableMap(states);
  }

  @Override
  public IncomingMessageEnvelope update(IncomingMessageEnvelope envelope) {
    EndOfStreamState state = inputStates.get(envelope.getSystemStreamPartition());
    EndOfStreamMessage message = (EndOfStreamMessage) envelope.getMessage();
    state.update(message.getTaskName(), message.getTaskCount());
    log.info("Received end-of-stream from task " + message.getTaskName() + " in " + envelope.getSystemStreamPartition());

    // If all the partitions for this system stream is end-of-stream, we create an aggregate
    // EndOfStream message for the streamId
    SystemStreamPartition ssp = envelope.getSystemStreamPartition();
    SystemStream systemStream = ssp.getSystemStream();
    if (isEndOfStream(systemStream)) {
      log.info("End-of-stream of input " + systemStream + " for " + ssp);
      EndOfStream eos = new EndOfStreamImpl(ssp, this);
      return new IncomingMessageEnvelope(ssp, IncomingMessageEnvelope.END_OF_STREAM_OFFSET, envelope.getKey(), eos);
    } else {
      return null;
    }
  }

  public boolean isEndOfStream(SystemStream systemStream) {
    return inputStates.entrySet().stream()
        .filter(entry -> entry.getKey().getSystemStream().equals(systemStream))
        .allMatch(entry -> entry.getValue().isEndOfStream());
  }

  public void sendEndOfStream(SystemStream systemStream) {
    log.info("Send end-of-stream messages to all partitions of " + systemStream);
    String stream = systemStream.getStream();
    SystemStreamMetadata metadata = sysAdmins.get(systemStream.getSystem())
        .getSystemStreamMetadata(Collections.singleton(stream))
        .get(stream);
    int partitionCount = metadata.getSystemStreamPartitionMetadata().size();
    for (int i = 0; i < partitionCount; i++) {
      String key = String.format(EOS_KEY_FORMAT, stream, taskName);
      EndOfStreamMessage message = new EndOfStreamMessage(taskName, taskCount);
      OutgoingMessageEnvelope envelopeOut = new OutgoingMessageEnvelope(systemStream, i, key, message);
      collector.send(envelopeOut);
    }
  }

  /**
   * Implementation of the EndOfStream object inside {@link IncomingMessageEnvelope}.
   * It wraps the end-of-stream ssp and the {@link EndOfStreamManager}.
   */
  private static final class EndOfStreamImpl implements EndOfStream {
    private final SystemStreamPartition ssp;
    private final EndOfStreamManager manager;

    private EndOfStreamImpl(SystemStreamPartition ssp, EndOfStreamManager manager) {
      this.ssp = ssp;
      this.manager = manager;
    }

    @Override
    public SystemStreamPartition get() {
      return ssp;
    }

    EndOfStreamManager getManager() {
      return manager;
    }
  }

  /**
   * This class keeps the internal state for a ssp to be end-of-stream.
   */
  private final static class EndOfStreamState {
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

  /**
   * This class propagates the end-of-stream control messages.
   * It uses the the input/output of the StreamGraph to decide the output intermediate streams, and shuts down
   * the task if all inputs reach end-of-stream.
   */
  public static final class EndOfStreamDispatcher {
    private final Multimap<SystemStream, IONode> ioGraph = HashMultimap.create();

    /**
     * Create the dispatcher for end-of-stream messages based on the IOGraph built on StreamGraph
     * @param streamGraph user {@link org.apache.samza.operators.StreamGraph}
     */
    private EndOfStreamDispatcher(StreamGraphImpl streamGraph) {
      streamGraph.toIOGraph().forEach(node -> {
          node.getInputs().forEach(stream -> {
              ioGraph.put(new SystemStream(stream.getSystemName(), stream.getPhysicalName()), node);
            });
        });
    }

    /**
     * Propagate end-of-stream to any downstream intermediate streams which have not reached end-of-stream.
     * If all the inputs have been end-of-stream, shut down the current task.
     *
     * @param endOfStream End-of-stream message
     * @param coordinator coordinator among tasks
     */
    public void propagate(EndOfStream endOfStream, TaskCoordinator coordinator) {
      EndOfStreamManager manager = ((EndOfStreamImpl) endOfStream).getManager();
      ioGraph.get(endOfStream.get().getSystemStream()).forEach(node -> {
        // find the intermediate streams that need broadcast the eos messages
        if (node.getOutputOpSpec().getOpCode() == OperatorSpec.OpCode.PARTITION_BY) {
          boolean inputsEndOfStream =
              node.getInputs().stream().allMatch(spec -> manager.isEndOfStream(spec.toSystemStream()));
          if (inputsEndOfStream) {
            // broadcast the end-of-stream message to the intermediate stream
            manager.sendEndOfStream(node.getOutput().toSystemStream());
          }
        }
      });

      boolean allEndOfStream = manager.inputStates.values().stream().allMatch(EndOfStreamState::isEndOfStream);
      if (allEndOfStream) {
        // all inputs have been end-of-stream, shut down the task
        log.info("All input streams have reached the end for task " + manager.taskName);
        coordinator.shutdown(TaskCoordinator.RequestScope.CURRENT_TASK);
      }
    }
  }

  /**
   * Builds an end-of-stream envelope for an SSP.
   *
   * @param ssp The SSP that is at end-of-stream.
   * @return an IncomingMessageEnvelope corresponding to end-of-stream for that SSP.
   */
  public static IncomingMessageEnvelope buildEndOfStreamEnvelope(SystemStreamPartition ssp) {
    return new IncomingMessageEnvelope(ssp, IncomingMessageEnvelope.END_OF_STREAM_OFFSET, null, new EndOfStreamMessage(null, 0));
  }

  /**
   * Create a end-of-stream dispatcher.
   * @param streamGraph user-defined {@link org.apache.samza.operators.StreamGraph}
   * @return the dispatcher
   */
  public static EndOfStreamDispatcher createDispatcher(StreamGraphImpl streamGraph) {
    return new EndOfStreamDispatcher(streamGraph);
  }
}
