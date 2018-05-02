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

package org.apache.samza.test.framework;

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.inmemory.InMemorySystemConsumer;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.test.framework.stream.CollectionStream;


/**
 * Provides general utilities for testing Streams
 */
public class StreamUtils {
  private static final Integer POLL_TIMEOUT = 10000;

  /**
   *
   * @param stream represents {@link CollectionStream} whose current state of partitions is requested to be fetched
   * @param <T> represents type of message
   * @return a map key of which represents the {@code partitionId} and value represents the current state of the partition
   *         i.e messages in the partition
   * @throws InterruptedException Thrown when a blocking poll has been interrupted by another thread.
   */
  public static <T> Map<Integer, List<T>> getStreamState(CollectionStream stream) throws InterruptedException {
    Preconditions.checkNotNull(stream);
    Preconditions.checkNotNull(stream.getCollectionStreamSystem());

    String streamName = stream.getStreamName();
    String systemName = stream.getCollectionStreamSystem().getSystemName();
    Set<SystemStreamPartition> ssps = new HashSet<>();
    Set<String> streamNames = new HashSet<>();
    streamNames.add(streamName);
    InMemorySystemFactory factory = stream.getCollectionStreamSystem().getFactory();
    Map<String, SystemStreamMetadata> metadata =
        factory.getAdmin(systemName, new MapConfig()).getSystemStreamMetadata(streamNames);
    InMemorySystemConsumer consumer = (InMemorySystemConsumer) factory.getConsumer(systemName, null, null);
    metadata.get(stream.getStreamName()).getSystemStreamPartitionMetadata().keySet().forEach(partition -> {
        SystemStreamPartition temp = new SystemStreamPartition(systemName, streamName, partition);
        ssps.add(temp);
        consumer.register(temp, "0");
      });

    long t = System.currentTimeMillis();
    Map<SystemStreamPartition, List<IncomingMessageEnvelope>> output = new HashMap<>();
    HashSet<SystemStreamPartition> didNotReachEndOfStream = new HashSet<>(ssps);
    while (System.currentTimeMillis() < t + POLL_TIMEOUT) {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> currentState = consumer.poll(ssps, 10);
      for (Map.Entry<SystemStreamPartition, List<IncomingMessageEnvelope>> entry : currentState.entrySet()) {
        SystemStreamPartition ssp = entry.getKey();
        output.putIfAbsent(ssp, new LinkedList<IncomingMessageEnvelope>());
        List<IncomingMessageEnvelope> currentBuffer = entry.getValue();
        Integer totalMessagesToFetch = Integer.valueOf(metadata.get(stream.getStreamName())
            .getSystemStreamPartitionMetadata()
            .get(ssp.getPartition())
            .getNewestOffset());
        if (output.get(ssp).size() + currentBuffer.size() == totalMessagesToFetch) {
          didNotReachEndOfStream.remove(entry.getKey());
          ssps.remove(entry.getKey());
        }
        output.get(ssp).addAll(currentBuffer);
      }

      if (didNotReachEndOfStream.isEmpty()) {
        break;
      }
    }

    if (!didNotReachEndOfStream.isEmpty()) {
      throw new IllegalStateException("Could not poll for all system stream partitions");
    }

    return output.entrySet()
        .stream()
        .collect(Collectors.toMap(entry -> entry.getKey().getPartition().getPartitionId(),
            entry -> entry.getValue().stream().map(e -> (T) e.getMessage()).collect(Collectors.toList())));
  }
}
