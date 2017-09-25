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
 *
 */
package org.apache.samza.operators.impl;

import org.apache.samza.config.MapConfig;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.storage.kv.KeyValueStoreMetrics;
import org.apache.samza.storage.kv.RocksDbKeyValueStore;
import org.rocksdb.*;

import java.io.File;

import java.util.Map;
import java.util.HashMap;

public class TestTimeSeriesStoreImpl {
  public void testTimeSeriesStore() {
    RocksDbKeyValueStore rocksKVStore = new RocksDbKeyValueStore(new File(System.getProperty("java.io.tmpdir")),
        new Options().setCreateIfMissing(true).setCompressionType(CompressionType.SNAPPY_COMPRESSION), new MapConfig(),
        false, "someStore", new WriteOptions(), new FlushOptions(),
        new KeyValueStoreMetrics("someStore", new MetricsRegistryMap()));

  }

  }
