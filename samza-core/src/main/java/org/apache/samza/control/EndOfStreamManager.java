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
import org.apache.samza.message.EndOfStreamMessage;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class handles the end-of-stream control message. It aggregates the end-of-stream state for each input ssps of
 * a task, and propagate the eos messages to downstream intermediate streams if needed.
 */
public class EndOfStreamManager {
  private static final Logger log = LoggerFactory.getLogger(EndOfStreamManager.class);
  private static final String EOS_KEY_FORMAT = "%s-%s-EOS"; //stream-task-EOS

  private final String taskName;
  private final MessageCollector collector;
  // end-of-stream state per ssp
  private final Map<SystemStreamPartition, EndOfStreamState> eosStates;
  private final StreamMetadataCache metadataCache;
  // mapping from input stream to its consuming tasks
  private final Multimap<SystemStream, String> streamToTasks;

  public EndOfStreamManager(String taskName,
      Multimap<SystemStream, String> streamToTasks,
      Set<SystemStreamPartition> ssps,
      StreamMetadataCache metadataCache,
      MessageCollector collector) {
    this.taskName = taskName;
    this.streamToTasks = streamToTasks;
    this.metadataCache = metadataCache;
    this.collector = collector;
    Map<SystemStreamPartition, EndOfStreamState> states = new HashMap<>();
    ssps.forEach(ssp -> {
        states.put(ssp, new EndOfStreamState());
      });
    this.eosStates = Collections.unmodifiableMap(states);
  }

  public EndOfStream update(IncomingMessageEnvelope envelope) {
    EndOfStreamState state = eosStates.get(envelope.getSystemStreamPartition());
    EndOfStreamMessage message = (EndOfStreamMessage) envelope.getMessage();
    state.update(message.getTaskName(), message.getTaskCount());
    log.info("Received end-of-stream from task " + message.getTaskName() + " in " + envelope.getSystemStreamPartition());

    // If all the partitions for this system stream is end-of-stream, we create an aggregate
    // EndOfStream message for the streamId
    SystemStreamPartition ssp = envelope.getSystemStreamPartition();
    SystemStream systemStream = ssp.getSystemStream();
    if (isEndOfStream(systemStream)) {
      log.info("End-of-stream of input " + systemStream + " for " + ssp);
      return new EndOfStreamImpl(systemStream, this);
    } else {
      return null;
    }
  }

  /**
   * Return true if all partitions of the systemStream that are assigned to the current task have reached EndOfStream.
   * @param systemStream stream
   * @return whether the stream reaches to the end for this task
   */
  public boolean isEndOfStream(SystemStream systemStream) {
    return eosStates.entrySet().stream()
        .filter(entry -> entry.getKey().getSystemStream().equals(systemStream))
        .allMatch(entry -> entry.getValue().isEndOfStream());
  }

  /**
   * Send the EndOfStream control messages to downstream
   * @param systemStream downstream stream
   * @param taskCount the number of upstream tasks that produces end-of-stream
   */
  public void sendEndOfStream(SystemStream systemStream, int taskCount) {
    log.info("Send end-of-stream messages to all partitions of " + systemStream);
    final EndOfStreamMessage message = new EndOfStreamMessage(taskName, taskCount);
    ControlMessageUtils.sendControlMessage(message, systemStream, metadataCache, collector);
  }

  /* package private */
  Map<SystemStreamPartition, EndOfStreamState> getEosStates() {
    return eosStates;
  }

  /* package private */
  Multimap<SystemStream, String> getStreamToTasks() {
    return streamToTasks;
  }

  /* package private */
  String getTaskName() {
    return taskName;
  }

  /**
   * Implementation of the EndOfStream object inside {@link IncomingMessageEnvelope}.
   * It wraps the end-of-stream ssp and the {@link EndOfStreamManager}.
   */
  /* package private */
  static final class EndOfStreamImpl implements EndOfStream {
    private final SystemStream systemStream;
    private final EndOfStreamManager manager;

    private EndOfStreamImpl(SystemStream systemStream, EndOfStreamManager manager) {
      this.systemStream = systemStream;
      this.manager = manager;
    }

    @Override
    public SystemStream getSystemStream() {
      return systemStream;
    }

    EndOfStreamManager getManager() {
      return manager;
    }
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

  /**
   * Build an end-of-stream envelope for an ssp of a source input.
   *
   * @param ssp The SSP that is at end-of-stream.
   * @return an IncomingMessageEnvelope corresponding to end-of-stream for that SSP.
   */
  public static IncomingMessageEnvelope buildEndOfStreamEnvelope(SystemStreamPartition ssp) {
    return new IncomingMessageEnvelope(ssp, IncomingMessageEnvelope.END_OF_STREAM_OFFSET, null, new EndOfStreamMessage(null, 0));
  }
}
