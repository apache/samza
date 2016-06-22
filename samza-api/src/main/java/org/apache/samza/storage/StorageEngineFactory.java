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

package org.apache.samza.storage;

import java.io.File;

import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.MessageCollector;

/**
 * An object provided by the storage engine implementation to create instances
 * of the given storage engine type.
 */
public interface StorageEngineFactory<K, V> {

  /**
   * Create an instance of the given storage engine.
   *
   * @param storeName The name of the storage engine.
   * @param storeDir The directory of the storage engine.
   * @param keySerde The serializer to use for serializing keys when reading or writing to the store.
   * @param msgSerde The serializer to use for serializing messages when reading or writing to the store.
   * @param collector MessageCollector the storage engine uses to persist changes.
   * @param registry MetricsRegistry to which to publish storage-engine specific metrics.
   * @param changeLogSystemStreamPartition Samza stream partition from which to receive the changelog.
   * @param containerContext Information about the container in which the task is executing.
   * @return The storage engine instance.
   */
  StorageEngine getStorageEngine(
    String storeName,
    File storeDir,
    Serde<K> keySerde,
    Serde<V> msgSerde,
    MessageCollector collector,
    MetricsRegistry registry,
    SystemStreamPartition changeLogSystemStreamPartition,
    SamzaContainerContext containerContext);
}
