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

package org.apache.samza.system.inmemory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;


/**
 *
 */
public class InMemorySystemConsumer implements SystemConsumer {
  private final InMemoryManager memoryManager;
  private final Map<SystemStreamPartition, String> sspToOffset;

  public InMemorySystemConsumer(InMemoryManager manager) {
    memoryManager = manager;
    sspToOffset = new ConcurrentHashMap<>();
  }

  /**
   * Tells the SystemConsumer to connect to the underlying system, and prepare
   * to begin serving messages when poll is invoked.
   */
  @Override
  public void start() {

  }

  /**
   * Tells the SystemConsumer to close all connections, release all resource,
   * and shut down everything. The SystemConsumer will not be used again after
   * stop is called.
   */
  @Override
  public void stop() {

  }

  /**
   * Register a SystemStreamPartition to this SystemConsumer. The SystemConsumer
   * should try and read messages from all SystemStreamPartitions that are
   * registered to it. SystemStreamPartitions should only be registered before
   * start is called.
   *  @param systemStreamPartition
   *          The SystemStreamPartition object representing the Samza
   *          SystemStreamPartition to receive messages from.
   * @param offset
   *          String representing the offset of the point in the stream to start
   *          reading messages from. This is an inclusive parameter; if "7" were
   *          specified, the first message for the system/stream/partition to be
   *          consumed and returned would be a message whose offset is "7".
   *          Note: For broadcast streams, different tasks may checkpoint the same ssp with different values. It
   */
  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {
    sspToOffset.put(systemStreamPartition, offset);
  }

  /**
   * Poll the SystemConsumer to get any available messages from the underlying
   * system.
   *
   * <p>
   * If the underlying implementation does not take care to adhere to the
   * timeout parameter, the SamzaContainer's performance will suffer
   * drastically. Specifically, if poll blocks when it's not supposed to, it
   * will block the entire main thread in SamzaContainer, and no messages will
   * be processed while blocking is occurring.
   * </p>
   *
   * @param systemStreamPartitions
   *          A set of SystemStreamPartition to poll for new messages. If
   *          SystemConsumer has messages available for other registered
   *          SystemStreamPartitions, but they are not in the
   *          systemStreamPartitions set in a given poll invocation, they can't
   *          be returned. It is illegal to pass in SystemStreamPartitions that
   *          have not been registered with the SystemConsumer first.
   * @param timeout
   *          If timeout &lt; 0, poll will block unless all SystemStreamPartition
   *          are at "head" (the underlying system has been checked, and
   *          returned an empty set). If at head, an empty map is returned. If
   *          timeout &gt;= 0, poll will return any messages that are currently
   *          available for any of the SystemStreamPartitions specified. If no
   *          new messages are available, it will wait up to timeout
   *          milliseconds for messages from any SystemStreamPartition to become
   *          available. It will return an empty map if the timeout is hit, and
   *          no new messages are available.
   * @return A map from SystemStreamPartitions to any available
   *         IncomingMessageEnvelopes for the SystemStreamPartitions. If no
   *         messages are available for a SystemStreamPartition that was
   *         supplied in the polling set, the map will not contain a key for the
   *         SystemStreamPartition. Will return an empty map, not null, if no
   *         new messages are available for any SystemStreamPartitions in the
   *         input set.
   * @throws InterruptedException
   *          Thrown when a blocking poll has been interrupted by another
   *          thread.
   */
  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
      Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
    Map<SystemStreamPartition, String> sspOffsetPairToFetch = sspToOffset.entrySet()
        .stream()
        .filter(entry -> systemStreamPartitions.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> result = memoryManager.poll(sspOffsetPairToFetch);

    for (Map.Entry<SystemStreamPartition, List<IncomingMessageEnvelope>> sspToMessage : result.entrySet()) {
      sspToOffset.computeIfPresent(sspToMessage.getKey(), (ssp, offset) -> {
          int newOffset = Integer.parseInt(offset) + sspToMessage.getValue().size();
          return String.valueOf(newOffset);
        });
      // absent should never be the case
    }

    return result;
  }
}
