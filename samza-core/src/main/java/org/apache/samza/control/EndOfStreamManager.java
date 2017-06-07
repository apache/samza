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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.samza.control.ControlMessageAggregator.ControlMessageManager;
import org.apache.samza.message.EndOfStreamMessage;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;

public class EndOfStreamManager implements ControlMessageManager {

  private final static class EndOfStreamState {
    private Set<String> tasks;
    private int expectedTotal = Integer.MAX_VALUE;
    private boolean isEndOfStream = false;

    void update(String taskName, int taskCount) {
      tasks.add(taskName);
      expectedTotal = taskCount;
      isEndOfStream = tasks.size() == expectedTotal;
    }

    boolean isEndOfStream() {
      return isEndOfStream;
    }
  }

  MessageCollector collector;
  Map<SystemStreamPartition, EndOfStreamState> inputStates = new HashMap<>();

  public EndOfStreamManager(Set<SystemStreamPartition> ssps, MessageCollector collector) {
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

    // if all the partitions for this system stream is end-of-stream, we generate
    // EndOfStream for the streamId
    SystemStream systemStream = envelope.getSystemStreamPartition().getSystemStream();
    if (isEndOfStream(systemStream)) {
      SystemStreamPartition ssp = new SystemStreamPartition(systemStream, null);
      EndOfStream eos = new EndOfStreamImpl();
      return new IncomingMessageEnvelope(ssp, IncomingMessageEnvelope.END_OF_STREAM_OFFSET, envelope.getKey(), eos);
    } else {
      return null;
    }
  }

  private boolean isEndOfStream(SystemStream systemStream) {
    return inputStates.entrySet().stream()
        .filter(entry -> entry.getKey().getSystemStream().equals(systemStream))
        .allMatch(entry -> entry.getValue().isEndOfStream());
  }

  private final class EndOfStreamImpl implements EndOfStream {
    @Override
    public boolean isEndOfStream(SystemStream systemStream) {
      return isEndOfStream(systemStream);
    }

    @Override
    public void updateEndOfStream(SystemStream systemStream) {
      //TODO: broadcast the watermark message to all the partitions of this system stream
    }
  }

  public static final class EndOfStreamDispatcher {
    public EndOfStreamDispatcher(StreamGraph streamGraph) {

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
}
