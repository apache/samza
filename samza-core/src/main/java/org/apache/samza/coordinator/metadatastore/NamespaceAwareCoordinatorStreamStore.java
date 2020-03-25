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

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.samza.coordinator.metadatastore.CoordinatorStreamStore.CoordinatorMessageKey;
import org.apache.samza.metadatastore.MetadataStore;

/**
 * Provides a namespace aware read/write operations on top of {@link CoordinatorStreamStore}.
 */
public class NamespaceAwareCoordinatorStreamStore implements MetadataStore {

  private final MetadataStore metadataStore;
  private final String namespace;

  /**
   * Instantiates the {@link NamespaceAwareCoordinatorStreamStore} based upon the provided
   * MetadataStore that is instantiated and the namespace.
   *
   * @param metadataStore the instantiated {@link MetadataStore}.
   * @param namespace the namespace to use for storing the keys in the metadata store.
   */
  public NamespaceAwareCoordinatorStreamStore(MetadataStore metadataStore, String namespace) {
    this.metadataStore = metadataStore;
    this.namespace = namespace;
  }

  @Override
  public void init() {
    // Metadata store lifecycle is managed outside of this class, so not starting it.
  }

  @Override
  public byte[] get(String key) {
    Map<String, byte[]> bootstrappedMessages = readMessagesFromCoordinatorStore();
    return bootstrappedMessages.get(key);
  }

  @Override
  public void put(String key, byte[] value) {
    String coordinatorMessageKeyAsJson = getCoordinatorMessageKey(key);
    metadataStore.put(coordinatorMessageKeyAsJson, value);
  }

  @Override
  public void putAll(Map<String, byte[]> entries) {
    Map<String, byte[]> mapWithCoordinatorMessageKeys =
        entries.entrySet().stream()
            .collect(Collectors.toMap(e -> getCoordinatorMessageKey(e.getKey()), e -> e.getValue()));
    metadataStore.putAll(mapWithCoordinatorMessageKeys);
  }

  @Override
  public void delete(String key) {
    String coordinatorMessageKeyAsJson = getCoordinatorMessageKey(key);
    metadataStore.delete(coordinatorMessageKeyAsJson);
  }

  @Override
  public Map<String, byte[]> all() {
    Map<String, byte[]> bootstrappedMessages = readMessagesFromCoordinatorStore();
    return Collections.unmodifiableMap(bootstrappedMessages);
  }

  @Override
  public void flush() {
    metadataStore.flush();
  }

  @Override
  public void close() {
    // Metadata store lifecycle is managed outside of this class, so not stopping it.
  }

  @VisibleForTesting
  String getCoordinatorMessageKey(String key) {
    return CoordinatorStreamStore.serializeCoordinatorMessageKeyToJson(namespace, key);
  }

  /**
   * Reads and returns all the messages from coordinator stream for a particular namespace.
   */
  private Map<String, byte[]> readMessagesFromCoordinatorStore() {
    Map<String, byte[]> bootstrappedMessages = new HashMap<>();
    Map<String, byte[]> coordinatorStreamMessages = metadataStore.all();
    coordinatorStreamMessages.forEach((coordinatorMessageKeyAsJson, value) -> {
        CoordinatorMessageKey coordinatorMessageKey = CoordinatorStreamStore.deserializeCoordinatorMessageKeyFromJson(coordinatorMessageKeyAsJson);
        if (Objects.equals(namespace, coordinatorMessageKey.getNamespace())) {
          if (value != null) {
            bootstrappedMessages.put(coordinatorMessageKey.getKey(), value);
          } else {
            bootstrappedMessages.remove(coordinatorMessageKey.getKey());
          }
        }
      });

    return bootstrappedMessages;
  }
}
