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
package org.apache.samza.coordinator.metadatastore;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionIterator;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link MetadataStore} interface where the metadata of the Samza job is stored in coordinator stream.
 *
 * This class is thread safe.
 */
public class CoordinatorStreamStore implements MetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorStreamStore.class);
  private static final String SOURCE = "SamzaContainer";

  private final Config config;
  private final SystemStream coordinatorSystemStream;
  private final SystemStreamPartition coordinatorSystemStreamPartition;
  private final SystemProducer systemProducer;
  private final SystemConsumer systemConsumer;
  private final SystemAdmin systemAdmin;
  private final String type;
  private final Serde<List<?>> keySerde;

  // Using custom comparator since java default comparator offers object identity equality(not value equality) for byte arrays.
  private final Map<byte[], byte[]> bootstrappedMessages = new TreeMap<>(UnsignedBytes.lexicographicalComparator());
  private final Object bootstrapLock = new Object();
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  private SystemStreamPartitionIterator iterator;

  public CoordinatorStreamStore(String namespace, Config config, MetricsRegistry metricsRegistry) {
    this.config = config;
    this.type = namespace;
    this.keySerde = new JsonSerde<>();
    this.coordinatorSystemStream = CoordinatorStreamUtil.getCoordinatorSystemStream(config);
    this.coordinatorSystemStreamPartition = new SystemStreamPartition(coordinatorSystemStream, new Partition(0));
    SystemFactory systemFactory = CoordinatorStreamUtil.getCoordinatorSystemFactory(config);
    this.systemProducer = systemFactory.getProducer(this.coordinatorSystemStream.getSystem(), config, metricsRegistry);
    this.systemConsumer = systemFactory.getConsumer(this.coordinatorSystemStream.getSystem(), config, metricsRegistry);
    this.systemAdmin = systemFactory.getAdmin(this.coordinatorSystemStream.getSystem(), config);
  }

  @Override
  public void init(Config config, MetricsRegistry metricsRegistry) {
    if (isInitialized.compareAndSet(false, true)) {
      LOG.info("Starting the coordinator stream system consumer with config: {}.", config);
      registerConsumer();
      systemConsumer.start();
      systemProducer.register(SOURCE);
      systemProducer.start();
      iterator = new SystemStreamPartitionIterator(systemConsumer, coordinatorSystemStreamPartition);
      bootstrapMessagesFromStream();
    } else {
      LOG.info("Store had already been initialized. Skipping.", coordinatorSystemStreamPartition);
    }
  }

  @Override
  public byte[] get(byte[] key) {
    bootstrapMessagesFromStream();
    return bootstrappedMessages.get(key);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(coordinatorSystemStream, 0, key, value);
    systemProducer.send(SOURCE, envelope);
    flush();
  }

  @Override
  public void delete(byte[] key) {
    // Since kafka doesn't support individual message deletion, store value as null for a key to delete.
    put(key, null);
  }

  @Override
  public Map<byte[], byte[]> all() {
    bootstrapMessagesFromStream();
    return Collections.unmodifiableMap(bootstrappedMessages);
  }

  /**
   * Returns all the messages from the earliest offset all the way to the latest.
   */
  private void bootstrapMessagesFromStream() {
    synchronized (bootstrapLock) {
      while (iterator.hasNext()) {
        IncomingMessageEnvelope envelope = iterator.next();
        byte[] keyAsBytes = (byte[]) envelope.getKey();
        Object[] keyArray = keySerde.fromBytes(keyAsBytes).toArray();
        CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyArray, new HashMap<>());
        if (Objects.equals(coordinatorStreamMessage.getType(), type)) {
          if (envelope.getMessage() != null) {
            bootstrappedMessages.put(keyAsBytes, (byte[]) envelope.getMessage());
          } else {
            bootstrappedMessages.remove(keyAsBytes);
          }
        }
      }
    }
  }

  @Override
  public void close() {
    try {
      LOG.info("Stopping the coordinator stream system consumer.", config);
      systemAdmin.stop();
      systemProducer.stop();
      systemConsumer.stop();
    } catch (Exception e) {
      LOG.error("Exception occurred when closing the metadata store:", e);
    }
  }

  @Override
  public void flush() {
    try {
      systemProducer.flush(SOURCE);
    } catch (Exception e) {
      LOG.error("Exception occurred when flushing the metadata store:", e);
    }
  }

  private void registerConsumer() {
    LOG.debug("Attempting to register system stream partition: {}", coordinatorSystemStreamPartition);
    String streamName = coordinatorSystemStreamPartition.getStream();
    Map<String, SystemStreamMetadata> systemStreamMetadataMap = systemAdmin.getSystemStreamMetadata(Sets.newHashSet(streamName));

    SystemStreamMetadata systemStreamMetadata = systemStreamMetadataMap.get(streamName);
    Preconditions.checkNotNull(systemStreamMetadata, String.format("System stream metadata does not exist for stream: %s.", streamName));

    SystemStreamPartitionMetadata systemStreamPartitionMetadata = systemStreamMetadata.getSystemStreamPartitionMetadata().get(coordinatorSystemStreamPartition.getPartition());
    Preconditions.checkNotNull(systemStreamPartitionMetadata, String.format("System stream partition metadata does not exist for: %s.", coordinatorSystemStreamPartition));

    String startingOffset = systemStreamPartitionMetadata.getOldestOffset();
    LOG.info("Registering system stream partition: {} with offset: {}.", coordinatorSystemStreamPartition, startingOffset);
    systemConsumer.register(coordinatorSystemStreamPartition, startingOffset);
  }
}
