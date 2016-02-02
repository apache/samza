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

package org.apache.samza.coordinator.stream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.Partition;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamMetadata.SystemStreamPartitionMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.system.SystemStreamPartitionIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper around a SystemConsumer that provides helpful methods for dealing
 * with the coordinator stream.
 */
public class CoordinatorStreamSystemConsumer {
  private static final Logger log = LoggerFactory.getLogger(CoordinatorStreamSystemConsumer.class);

  private final Serde<List<?>> keySerde;
  private final Serde<Map<String, Object>> messageSerde;
  private final SystemStreamPartition coordinatorSystemStreamPartition;
  private final SystemConsumer systemConsumer;
  private final SystemAdmin systemAdmin;
  private final Map<String, String> configMap;
  private boolean isBootstrapped;
  private boolean isStarted;
  private Set<CoordinatorStreamMessage> bootstrappedStreamSet = new LinkedHashSet<CoordinatorStreamMessage>();

  public CoordinatorStreamSystemConsumer(SystemStream coordinatorSystemStream, SystemConsumer systemConsumer, SystemAdmin systemAdmin, Serde<List<?>> keySerde, Serde<Map<String, Object>> messageSerde) {
    this.coordinatorSystemStreamPartition = new SystemStreamPartition(coordinatorSystemStream, new Partition(0));
    this.systemConsumer = systemConsumer;
    this.systemAdmin = systemAdmin;
    this.configMap = new HashMap<String, String>();
    this.isBootstrapped = false;
    this.keySerde = keySerde;
    this.messageSerde = messageSerde;
  }

  public CoordinatorStreamSystemConsumer(SystemStream coordinatorSystemStream, SystemConsumer systemConsumer, SystemAdmin systemAdmin) {
    this(coordinatorSystemStream, systemConsumer, systemAdmin, new JsonSerde<List<?>>(), new JsonSerde<Map<String, Object>>());
  }

  /**
   * Retrieves the oldest offset in the coordinator stream, and registers the
   * coordinator stream with the SystemConsumer using the earliest offset.
   */
  public void register() {
    if (isStarted) {
      log.info("Coordinator stream partition {} has already been registered. Skipping.", coordinatorSystemStreamPartition);
      return;
    }
    log.debug("Attempting to register: {}", coordinatorSystemStreamPartition);
    Set<String> streamNames = new HashSet<String>();
    String streamName = coordinatorSystemStreamPartition.getStream();
    streamNames.add(streamName);
    Map<String, SystemStreamMetadata> systemStreamMetadataMap = systemAdmin.getSystemStreamMetadata(streamNames);

    if (systemStreamMetadataMap == null) {
      throw new SamzaException("Received a null systemStreamMetadataMap from the systemAdmin. This is illegal.");
    }

    SystemStreamMetadata systemStreamMetadata = systemStreamMetadataMap.get(streamName);

    if (systemStreamMetadata == null) {
      throw new SamzaException("Expected " + streamName + " to be in system stream metadata.");
    }

    SystemStreamPartitionMetadata systemStreamPartitionMetadata = systemStreamMetadata.getSystemStreamPartitionMetadata().get(coordinatorSystemStreamPartition.getPartition());

    if (systemStreamPartitionMetadata == null) {
      throw new SamzaException("Expected metadata for " + coordinatorSystemStreamPartition + " to exist.");
    }

    String startingOffset = systemStreamPartitionMetadata.getOldestOffset();
    log.debug("Registering {} with offset {}", coordinatorSystemStreamPartition, startingOffset);
    systemConsumer.register(coordinatorSystemStreamPartition, startingOffset);
  }

  /**
   * Starts the underlying SystemConsumer.
   */
  public void start() {
    if (isStarted) {
      log.info("Coordinator stream consumer already started");
      return;
    }
    log.info("Starting coordinator stream system consumer.");
    systemConsumer.start();
    isStarted = true;
  }

  /**
   * Stops the underlying SystemConsumer.
   */
  public void stop() {
    log.info("Stopping coordinator stream system consumer.");
    systemConsumer.stop();
    isStarted = false;
  }

  /**
   * Read all messages from the earliest offset, all the way to the latest.
   * Currently, this method only pays attention to config messages.
   */
  public void bootstrap() {
    log.info("Bootstrapping configuration from coordinator stream.");
    SystemStreamPartitionIterator iterator = new SystemStreamPartitionIterator(systemConsumer, coordinatorSystemStreamPartition);

    try {
      while (iterator.hasNext()) {
        IncomingMessageEnvelope envelope = iterator.next();
        Object[] keyArray = keySerde.fromBytes((byte[]) envelope.getKey()).toArray();
        Map<String, Object> valueMap = null;
        if (envelope.getMessage() != null) {
          valueMap = messageSerde.fromBytes((byte[]) envelope.getMessage());
        }
        CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyArray, valueMap);
        log.debug("Received coordinator stream message: {}", coordinatorStreamMessage);
        bootstrappedStreamSet.add(coordinatorStreamMessage);
        if (SetConfig.TYPE.equals(coordinatorStreamMessage.getType())) {
          String configKey = coordinatorStreamMessage.getKey();
          if (coordinatorStreamMessage.isDelete()) {
            configMap.remove(configKey);
          } else {
            String configValue = new SetConfig(coordinatorStreamMessage).getConfigValue();
            configMap.put(configKey, configValue);
          }
        }
      }
      log.debug("Bootstrapped configuration: {}", configMap);
      isBootstrapped = true;
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  public Set<CoordinatorStreamMessage> getBoostrappedStream() {
    log.info("Returning the bootstrapped data from the stream");
    if (!isBootstrapped)
      bootstrap();
    return bootstrappedStreamSet;
  }

  public Set<CoordinatorStreamMessage> getBootstrappedStream(String type) {
    log.debug("Bootstrapping coordinator stream for messages of type {}", type);
    bootstrap();
    LinkedHashSet<CoordinatorStreamMessage> bootstrappedStream = new LinkedHashSet<CoordinatorStreamMessage>();
    for (CoordinatorStreamMessage coordinatorStreamMessage : bootstrappedStreamSet) {
      if (type.equalsIgnoreCase(coordinatorStreamMessage.getType())) {
        bootstrappedStream.add(coordinatorStreamMessage);
      }
    }
    return bootstrappedStream;
  }

  /**
   * @return The bootstrapped configuration that's been read after bootstrap has
   * been invoked.
   */
  public Config getConfig() {
    if (isBootstrapped) {
      return new MapConfig(configMap);
    } else {
      throw new SamzaException("Must call bootstrap before retrieving config.");
    }
  }

  /**
   * Gets an iterator on the coordinator stream, starting from the starting offset the consumer was registered with.
   *
   * @return an iterator on the coordinator stream pointing to the starting offset the consumer was registered with.
   */
  public SystemStreamPartitionIterator getStartIterator() {
    return new SystemStreamPartitionIterator(systemConsumer, coordinatorSystemStreamPartition);
  }

  /**
   * returns all unread messages after an iterator on the stream
   *
   * @param iterator the iterator pointing to an offset in the coordinator stream. All unread messages after this iterator are returned
   * @return a set of unread messages after a given iterator
   */
  public Set<CoordinatorStreamMessage> getUnreadMessages(SystemStreamPartitionIterator iterator) {
    return getUnreadMessages(iterator, null);
  }

  /**
   * returns all unread messages of a specific type, after an iterator on the stream
   *
   * @param iterator the iterator pointing to an offset in the coordinator stream. All unread messages after this iterator are returned
   * @param type     the type of the messages to be returned
   * @return a set of unread messages of a given type, after a given iterator
   */
  public Set<CoordinatorStreamMessage> getUnreadMessages(SystemStreamPartitionIterator iterator, String type) {
    LinkedHashSet<CoordinatorStreamMessage> messages = new LinkedHashSet<CoordinatorStreamMessage>();
    while (iterator.hasNext()) {
      IncomingMessageEnvelope envelope = iterator.next();
      Object[] keyArray = keySerde.fromBytes((byte[]) envelope.getKey()).toArray();
      Map<String, Object> valueMap = null;
      if (envelope.getMessage() != null) {
        valueMap = messageSerde.fromBytes((byte[]) envelope.getMessage());
      }
      CoordinatorStreamMessage coordinatorStreamMessage = new CoordinatorStreamMessage(keyArray, valueMap);
      if (type == null || type.equals(coordinatorStreamMessage.getType())) {
        messages.add(coordinatorStreamMessage);
      }
    }
    return messages;
  }

  /**
   * Checks whether or not there are any messages after a given iterator on the coordinator stream
   *
   * @param iterator The iterator to check if there are any new messages after this point
   * @return True if there are new messages after the iterator, false otherwise
   */
  public boolean hasNewMessages(SystemStreamPartitionIterator iterator) {
    if (iterator == null) {
      return false;
    }
    return iterator.hasNext();
  }

}
