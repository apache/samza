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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.CoordinatorStreamKeySerde;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.metadatastore.MetadataStore;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionIterator;
import org.apache.samza.util.CoordinatorStreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the {@link MetadataStore} interface where the metadata of the samza job is stored in coordinator stream.
 *
 * This class is thread safe.
 *
 * It is recommended to use {@link NamespaceAwareCoordinatorStreamStore}. This will enable the single CoordinatorStreamStore connection
 * to be shared by the multiple {@link NamespaceAwareCoordinatorStreamStore} instances.
 */
public class CoordinatorStreamStore implements MetadataStore {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorStreamStore.class);
  private static final String SOURCE = "SamzaContainer";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final Config config;
  private final SystemStream coordinatorSystemStream;
  private final SystemStreamPartition coordinatorSystemStreamPartition;
  private final SystemProducer systemProducer;
  private final SystemConsumer systemConsumer;
  private final SystemAdmin systemAdmin;

  // Namespaced key to the message byte array.
  private final Map<String, byte[]> messagesReadFromCoordinatorStream = new ConcurrentHashMap<>();

  private final Object bootstrapLock = new Object();
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  private SystemStreamPartitionIterator iterator;

  public CoordinatorStreamStore(Config config, MetricsRegistry metricsRegistry) {
    this.config = config;
    this.coordinatorSystemStream = CoordinatorStreamUtil.getCoordinatorSystemStream(config);
    this.coordinatorSystemStreamPartition = new SystemStreamPartition(coordinatorSystemStream, new Partition(0));
    SystemFactory systemFactory = CoordinatorStreamUtil.getCoordinatorSystemFactory(config);
    this.systemProducer = systemFactory.getProducer(this.coordinatorSystemStream.getSystem(), config, metricsRegistry, this.getClass().getSimpleName());
    this.systemConsumer = systemFactory.getConsumer(this.coordinatorSystemStream.getSystem(), config, metricsRegistry, this.getClass().getSimpleName());
    this.systemAdmin = systemFactory.getAdmin(this.coordinatorSystemStream.getSystem(), config, this.getClass().getSimpleName());
  }

  @VisibleForTesting
  protected CoordinatorStreamStore(Config config, SystemProducer systemProducer, SystemConsumer systemConsumer, SystemAdmin systemAdmin) {
    this.config = config;
    this.systemConsumer = systemConsumer;
    this.systemProducer = systemProducer;
    this.systemAdmin = systemAdmin;
    this.coordinatorSystemStream = CoordinatorStreamUtil.getCoordinatorSystemStream(config);
    this.coordinatorSystemStreamPartition = new SystemStreamPartition(coordinatorSystemStream, new Partition(0));
  }

  @Override
  public void init() {
    if (isInitialized.compareAndSet(false, true)) {
      LOG.info("Starting the coordinator stream system consumer with config: {}.", config);
      registerConsumer();
      systemConsumer.start();
      systemProducer.register(SOURCE);
      systemProducer.start();
      iterator = new SystemStreamPartitionIterator(systemConsumer, coordinatorSystemStreamPartition);
      readMessagesFromCoordinatorStream();
    } else {
      LOG.info("Store had already been initialized. Skipping.", coordinatorSystemStreamPartition);
    }
  }

  @Override
  public byte[] get(String namespacedKey) {
    readMessagesFromCoordinatorStream();
    return messagesReadFromCoordinatorStream.get(namespacedKey);
  }

  @Override
  public void put(String namespacedKey, byte[] value) {
    // 1. Store the namespace and key into correct fields of the CoordinatorStreamKey and convert the key to bytes.
    CoordinatorMessageKey coordinatorMessageKey = deserializeCoordinatorMessageKeyFromJson(namespacedKey);
    CoordinatorStreamKeySerde keySerde = new CoordinatorStreamKeySerde(coordinatorMessageKey.getNamespace());
    byte[] keyBytes = keySerde.toBytes(coordinatorMessageKey.getKey());

    // 2. Set the key, message in correct fields of {@link OutgoingMessageEnvelope} and publish it to the coordinator stream.
    OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(coordinatorSystemStream, 0, keyBytes, value);
    systemProducer.send(SOURCE, envelope);
  }

  @Override
  public void delete(String namespacedKey) {
    // Since kafka doesn't support individual message deletion, store value as null for a namespacedKey to delete.
    put(namespacedKey, null);
  }

  @Override
  public Map<String, byte[]> all() {
    readMessagesFromCoordinatorStream();
    return Collections.unmodifiableMap(messagesReadFromCoordinatorStream);
  }

  private void readMessagesFromCoordinatorStream() {
    synchronized (bootstrapLock) {
      while (iterator.hasNext()) {
        IncomingMessageEnvelope envelope = iterator.next();
        byte[] keyAsBytes = (byte[]) envelope.getKey();
        Serde<List<?>> serde = new JsonSerde<>();
        Object[] keyArray = serde.fromBytes(keyAsBytes).toArray();
        CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyArray, new HashMap<>());
        String namespacedKey = serializeCoordinatorMessageKeyToJson(coordinatorStreamMessage.getType(), coordinatorStreamMessage.getKey());
        if (envelope.getMessage() != null) {
          messagesReadFromCoordinatorStream.put(namespacedKey, (byte[]) envelope.getMessage());
        } else {
          messagesReadFromCoordinatorStream.remove(namespacedKey);
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
      throw new SamzaException("Exception occurred when flushing the metadata store:", e);
    }
  }

  /**
   * <p>
   *   Fetches the metadata of the topic partition of coordinator stream. Registers the oldest offset
   *   for the topic partition of coordinator stream with the coordinator system consumer.
   * </p>
   */
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

  /**
   *
   * Serializes the {@link CoordinatorMessageKey} into a json string.
   *
   * @param type the type of the coordinator message.
   * @param key the key associated with the type
   * @return the CoordinatorMessageKey serialized to a json string.
   */
  public static String serializeCoordinatorMessageKeyToJson(String type, String key) {
    try {
      CoordinatorMessageKey coordinatorMessageKey = new CoordinatorMessageKey(key, type);
      return OBJECT_MAPPER.writeValueAsString(coordinatorMessageKey);
    } catch (IOException e) {
      throw new SamzaException(String.format("Exception occurred when serializing metadata for type: %s, key: %s", type, key), e);
    }
  }

  /**
   * Deserializes the @param coordinatorMsgKeyAsString in json format to {@link CoordinatorMessageKey}.
   * @param coordinatorMsgKeyAsJson the serialized CoordinatorMessageKey in json format.
   * @return the deserialized CoordinatorMessageKey.
   */
  public static CoordinatorMessageKey deserializeCoordinatorMessageKeyFromJson(String coordinatorMsgKeyAsJson) {
    try {
      return OBJECT_MAPPER.readValue(coordinatorMsgKeyAsJson, CoordinatorMessageKey.class);
    } catch (IOException e) {
      throw new SamzaException(String.format("Exception occurred when deserializing the coordinatorMsgKey: %s", coordinatorMsgKeyAsJson), e);
    }
  }

  /**
   * <p>
   * Represents the key of a message in the coordinator stream.
   *
   * Coordinator message key is composite. It has both the type of the message
   * and the key associated with the type in it.
   * </p>
   */
  public static class CoordinatorMessageKey {

    // Represents the key associated with the type
    private final String key;

    // Represents the type of the message.
    private final String namespace;

    CoordinatorMessageKey(@JsonProperty("key") String key,
                          @JsonProperty("namespace") String namespace) {
      this.key = key;
      this.namespace = namespace;
    }

    public String getKey() {
      return this.key;
    }

    public String getNamespace() {
      return this.namespace;
    }
  }
}
