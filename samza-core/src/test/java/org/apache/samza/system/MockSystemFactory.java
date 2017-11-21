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

package org.apache.samza.system;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;


/**
 * A mock system backed by a set of in-memory queues. Used for testing w/o actual external messaging systems.
 */
public class MockSystemFactory implements SystemFactory {

  public static final Map<SystemStreamPartition, List<IncomingMessageEnvelope>> MSG_QUEUES = new ConcurrentHashMap<>();

  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    return new SystemConsumer() {
      public void start() {
      }

      public void stop() {
      }

      public void register(SystemStreamPartition systemStreamPartition, String offset) {
        MSG_QUEUES.putIfAbsent(systemStreamPartition, new ArrayList<>());
      }

      public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) {
        Map<SystemStreamPartition, List<IncomingMessageEnvelope>> retQueues = new HashMap<>();
        systemStreamPartitions.forEach(ssp -> {
            List<IncomingMessageEnvelope> msgs = MSG_QUEUES.get(ssp);
            if (msgs == null) {
              retQueues.put(ssp, new ArrayList<>());
            } else {
              retQueues.put(ssp, MSG_QUEUES.remove(ssp));
            }
          });
        return retQueues;
      }
    };
  }

  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new SystemProducer() {
      private final Random seed = new Random(System.currentTimeMillis());

      @Override
      public void start() {

      }

      @Override
      public void stop() {

      }

      @Override
      public void register(String source) {
      }

      @Override
      public void send(String source, OutgoingMessageEnvelope envelope) {
        SystemStream systemStream = envelope.getSystemStream();
        List<SystemStreamPartition> sspForSystem = MSG_QUEUES.keySet().stream()
            .filter(ssp -> ssp.getSystemStream().equals(systemStream))
            .collect(ArrayList::new, (l, ssp) -> l.add(ssp), (l1, l2) -> l1.addAll(l2));
        if (sspForSystem.isEmpty()) {
          MSG_QUEUES.putIfAbsent(new SystemStreamPartition(systemStream, new Partition(0)), new ArrayList<>());
          sspForSystem.add(new SystemStreamPartition(systemStream, new Partition(0)));
        }
        int partitionCount = sspForSystem.size();
        int partitionId = envelope.getPartitionKey() == null ?
            envelope.getKey() == null ? this.seed.nextInt(partitionCount) : envelope.getKey().hashCode() % partitionCount :
            envelope.getPartitionKey().hashCode() % partitionCount;
        SystemStreamPartition ssp = new SystemStreamPartition(envelope.getSystemStream(), new Partition(partitionId));
        List<IncomingMessageEnvelope> msgQueue = MSG_QUEUES.get(ssp);
        msgQueue.add(new IncomingMessageEnvelope(ssp, null, envelope.getKey(), envelope.getMessage()));
      }

      @Override
      public void flush(String source) {

      }
    };
  }

  public SystemAdmin getAdmin(String systemName, Config config) {
    return new ExtendedSystemAdmin() {

      @Override
      public Map<SystemStreamPartition, String> getOffsetsAfter(Map<SystemStreamPartition, String> offsets) {
        return null;
      }

      @Override
      public Map<String, SystemStreamMetadata> getSystemStreamMetadata(Set<String> streamNames) {
        Map<String, SystemStreamMetadata> metadataMap = new HashMap<>();
        Map<String, Set<Partition>> partitionMap = MSG_QUEUES.entrySet()
            .stream()
            .filter(entry -> streamNames.contains(entry.getKey().getSystemStream().getStream()))
            .map(e -> e.getKey()).<Map<String, Set<Partition>>>collect(HashMap::new, (m, ssp) -> {
                if (m.get(ssp.getStream()) == null) {
                  m.put(ssp.getStream(), new HashSet<>());
                }
                m.get(ssp.getStream()).add(ssp.getPartition());
              }, (m1, m2) -> {
                m2.forEach((k, v) -> {
                    if (m1.get(k) == null) {
                      m1.put(k, v);
                    } else {
                      m1.get(k).addAll(v);
                    }
                  });
              });

        partitionMap.forEach((k, v) -> {
            Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata> partitionMetaMap =
                v.stream().<Map<Partition, SystemStreamMetadata.SystemStreamPartitionMetadata>>collect(HashMap::new,
                  (m, p) -> {
                    m.put(p, new SystemStreamMetadata.SystemStreamPartitionMetadata("", "", ""));
                  }, (m1, m2) -> m1.putAll(m2));

            metadataMap.put(k, new SystemStreamMetadata(k, partitionMetaMap));
          });

        return metadataMap;
      }

      @Override
      public Integer offsetComparator(String offset1, String offset2) {
        return null;
      }

      @Override
      public Map<String, SystemStreamMetadata> getSystemStreamPartitionCounts(Set<String> streamNames, long cacheTTL) {
        return getSystemStreamMetadata(streamNames);
      }

      @Override
      public String getNewestOffset(SystemStreamPartition ssp, Integer maxRetries) {
        return null;
      }

      @Override
      public boolean createStream(StreamSpec streamSpec) {
        return true;
      }

      @Override
      public void validateStream(StreamSpec streamSpec) throws StreamValidationException {

      }
    };
  }
}