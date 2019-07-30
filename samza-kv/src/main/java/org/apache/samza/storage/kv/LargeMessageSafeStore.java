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
package org.apache.samza.storage.kv;

import java.util.ArrayList;
import java.util.List;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class checks for the size of the message being stored and either throws an exception when a large message is
 * encountered or ignores the large message and continues processing, depending on how it is configured.
 */
public class LargeMessageSafeStore implements KeyValueStore<byte[], byte[]> {

  private static final Logger LOG = LoggerFactory.getLogger(LargeMessageSafeStore.class);
  private final KeyValueStore<byte[], byte[]> store;
  private final String storeName;
  private final boolean dropLargeMessages;
  private final int maxMessageSize;
  private final LargeMessageSafeStoreMetrics largeMessageSafeStoreMetrics;

  public LargeMessageSafeStore(KeyValueStore<byte[], byte[]> store, String storeName, boolean dropLargeMessages, int maxMessageSize) {
    this.store = store;
    this.storeName = storeName;
    this.dropLargeMessages = dropLargeMessages;
    this.maxMessageSize = maxMessageSize;
    this.largeMessageSafeStoreMetrics = new LargeMessageSafeStoreMetrics(storeName, new MetricsRegistryMap());
  }

  @Override
  public byte[] get(byte[] key) {
    return store.get(key);
  }

  /**
   * This function puts a message in the store after validating its size.
   * It drops the large message if it has been configured to do so.
   * Otherwise, it throws an exception stating that a large message was encountered.
   *
   * @param key the key with which the specified {@code value} is to be associated.
   * @param value the value with which the specified {@code key} is to be associated.
   */
  @Override
  public void put(byte[] key, byte[] value) {
    validateMessageSize(value);
    if (!isLargeMessage(value)) {
      store.put(key, value);
    } else {
      LOG.info("Ignoring a large message with size " + value.length + " since it is greater than "
          + "the maximum allowed value of " + maxMessageSize);
      largeMessageSafeStoreMetrics.ignoredLargeMessages().inc();
    }
  }

  /**
   * This function puts messages in the store after validating their size.
   * It drops any large messages in the entry list if it has been configured to do so.
   * Otherwise, it throws an exception stating that a large message was encountered,
   * and does not put any of the messages in the store.
   *
   * @param entries the updated mappings to put into this key-value store.
   */
  @Override
  public void putAll(List<Entry<byte[], byte[]>> entries) {
    entries.forEach(entry -> {
        validateMessageSize(entry.getValue());
      });
    List<Entry<byte[], byte[]>> largeMessageSafeEntries = removeLargeMessages(entries);
    store.putAll(largeMessageSafeEntries);
  }

  @Override
  public void delete(byte[] key) {
    store.delete(key);
  }

  @Override
  public void deleteAll(List<byte[]> keys) {
    store.deleteAll(keys);
  }

  @Override
  public KeyValueIterator<byte[], byte[]> range(byte[] from, byte[] to) {
    return store.range(from, to);
  }

  @Override
  public KeyValueSnapshot<byte[], byte[]> snapshot(byte[] from, byte[] to) {
    return store.snapshot(from, to);
  }

  @Override
  public KeyValueIterator<byte[], byte[]> all() {
    return store.all();
  }

  @Override
  public void close() {
    store.close();
  }

  @Override
  public void flush() {
    store.flush();
  }

  private void validateMessageSize(byte[] message) {
    if (!dropLargeMessages && isLargeMessage(message)) {
      throw new RecordTooLargeException("The message size " + message.length + " for store " + storeName
          + " was larger than the maximum allowed message size " + maxMessageSize + ".");
    }
  }

  private boolean isLargeMessage(byte[] message) {
    return message != null && message.length > maxMessageSize;
  }

  private List<Entry<byte[], byte[]>> removeLargeMessages(List<Entry<byte[], byte[]>> entries) {
    List<Entry<byte[], byte[]>> largeMessageSafeEntries = new ArrayList<>();
    entries.forEach(entry -> {
        if (!isLargeMessage(entry.getValue())) {
          largeMessageSafeEntries.add(entry);
        } else {
          LOG.info("Ignoring a large message with size " + entry.getValue().length + " since it is greater than "
              + "the maximum allowed value of " + maxMessageSize);
          largeMessageSafeStoreMetrics.ignoredLargeMessages().inc();
        }
      });
    return largeMessageSafeEntries;
  }
}
