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
package org.apache.samza.metadatastore;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * In-memory {@link MetadataStore} with no persistence on disk.
 */
public class InMemoryMetadataStore implements MetadataStore {

  private final ConcurrentHashMap<String, byte[]> memStore = new ConcurrentHashMap<>();

  @Override
  public void init() { }

  @Override
  public byte[] get(String key) {
    return memStore.get(key);
  }

  @Override
  public void put(String key, byte[] value) {
    memStore.put(key, value);
  }

  @Override
  public void delete(String key) {
    memStore.remove(key);
  }

  @Override
  public Map<String, byte[]> all() {
    return ImmutableMap.copyOf(memStore);
  }

  @Override
  public void flush() { }

  @Override
  public void close() { }
}
