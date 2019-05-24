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
import org.apache.samza.SamzaException;


public class LargeMessageSafeStore implements KeyValueStore<byte[], byte[]> {

  private KeyValueStore<byte[], byte[]> store;
  private String storeName;
  private Boolean dropLargeMessage;
  private int maxMessageSize;

  public LargeMessageSafeStore(KeyValueStore<byte[], byte[]> store, String storeName, Boolean dropLargeMessage, int maxMessageSize) {
    this.store = store;
    this.storeName = storeName;
    this.dropLargeMessage = dropLargeMessage;
    this.maxMessageSize = maxMessageSize;
  }

  @Override
  public byte[] get(byte[] key) {
    return store.get(key);
  }

  @Override
  public void put(byte[] key, byte[] value) {
    validateMessageSize(value);
    if (!isLargeMessage(value)) {
      store.put(key, value);
    }
  }

  @Override
  public void putAll(List<Entry<byte[], byte[]>> entries) {
    entries.forEach(entry -> {
        validateMessageSize(entry.getValue());
      });
    List<Entry<byte[], byte[]>> largeMessageSafeEntries = removeLargeMessage(entries);
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
    if (!dropLargeMessage && isLargeMessage(message)) {
      throw new SamzaException("Logged store message size " + message.length + " for store " + storeName
          + " was larger than the maximum allowed message size " + maxMessageSize + ".");
    }
  }

  private boolean isLargeMessage(byte[] message) {
    return message != null && message.length > maxMessageSize;
  }

  private List<Entry<byte[], byte[]>> removeLargeMessage(List<Entry<byte[], byte[]>> entries) {
    List<Entry<byte[], byte[]>> largeMessageSafeEntries = new ArrayList<>();
    entries.forEach(entry -> {
        if (!isLargeMessage(entry.getValue())) {
          largeMessageSafeEntries.add(entry);
        }
      });
    return largeMessageSafeEntries;
  }
}
