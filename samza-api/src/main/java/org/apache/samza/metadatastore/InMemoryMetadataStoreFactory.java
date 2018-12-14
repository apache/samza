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

import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;


/**
 * Factory for an in-memory {@link MetadataStore}. Not intended for use in production.
 */
public class InMemoryMetadataStoreFactory implements MetadataStoreFactory {

  private static final ConcurrentHashMap<String, InMemoryMetadataStore> NAMESPACED_STORES = new ConcurrentHashMap<>();

  @Override
  public MetadataStore getMetadataStore(String namespace, Config config, MetricsRegistry metricsRegistry) {
    NAMESPACED_STORES.putIfAbsent(namespace, new InMemoryMetadataStore());
    return NAMESPACED_STORES.get(namespace);
  }
}
