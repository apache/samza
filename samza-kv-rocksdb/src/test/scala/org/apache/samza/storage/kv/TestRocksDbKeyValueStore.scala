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

package org.apache.samza.storage.kv


import java.io.File
import java.util

import org.apache.samza.SamzaException
import org.apache.samza.config.MapConfig
import org.apache.samza.metrics.{Gauge, MetricsRegistryMap}
import org.apache.samza.util.ExponentialSleepStrategy
import org.junit.{Assert, Test}
import org.rocksdb.{FlushOptions, Options, RocksDB, RocksIterator}

class TestRocksDbKeyValueStore
{
  @Test
  def testTTL() {
    val map = new java.util.HashMap[String, String]()
    map.put("rocksdb.ttl.ms", "1000")
    val config = new MapConfig(map)
    val options = new Options()
    options.setCreateIfMissing(true)
    val rocksDB = RocksDbKeyValueStore.openDB(new File(System.getProperty("java.io.tmpdir")),
                                              options,
                                              config,
                                              false,
                                              "someStore",
                                              null)
    val key = "test".getBytes("UTF-8")
    rocksDB.put(key, "val".getBytes("UTF-8"))
    Assert.assertNotNull(rocksDB.get(key))
    val retryBackoff: ExponentialSleepStrategy = new ExponentialSleepStrategy
    retryBackoff.run(loop => {
      if(rocksDB.get(key) == null) {
        loop.done
      }
      rocksDB.compactRange()
    }, (exception, loop) => {
      exception match {
        case e: Exception =>
          loop.done
          throw e
      }
    })
    Assert.assertNull(rocksDB.get(key))
    rocksDB.close()
  }

  @Test
  def testFlush(): Unit = {
    val map = new util.HashMap[String, String]()
    val config = new MapConfig(map)
    val options = new Options()
    options.setCreateIfMissing(true)
    val rocksDB = RocksDbKeyValueStore.openDB(new File(System.getProperty("java.io.tmpdir")),
                                              options,
                                              config,
                                              false,
                                              "dbStore",
                                              null)
    val key = "key".getBytes("UTF-8")
    rocksDB.put(key, "val".getBytes("UTF-8"))
    // SAMZA-836: Mysteriously,calling new FlushOptions() does not invoke the NativeLibraryLoader in rocksdbjni-3.13.1!
    // Moving this line after calling new Options() resolve the issue.
    val flushOptions = new FlushOptions().setWaitForFlush(true)
    rocksDB.flush(flushOptions)
    val dbDir = new File(System.getProperty("java.io.tmpdir")).toString
    val rocksDBReadOnly = RocksDB.openReadOnly(options, dbDir)
    Assert.assertEquals(new String(rocksDBReadOnly.get(key), "UTF-8"), "val")
    rocksDB.close()
    rocksDBReadOnly.close()
  }

  @Test(expected = classOf[SamzaException])
  def testFlushAfterCloseThrowsException(): Unit = {
    val map = new util.HashMap[String, String]()
    val config = new MapConfig(map)
    val options = new Options()
    options.setCreateIfMissing(true)

    val dbDir = new File(System.getProperty("java.io.tmpdir"))
    val rocksDB = new RocksDbKeyValueStore(dbDir, options, config, false, "dbStore")

    val key = "key".getBytes("UTF-8")
    rocksDB.put(key, "val".getBytes("UTF-8"))

    rocksDB.close()
    rocksDB.flush()
  }

  @Test(expected = classOf[SamzaException])
  def testGetAfterCloseThrowsException(): Unit = {
    val map = new util.HashMap[String, String]()
    val config = new MapConfig(map)
    val options = new Options()
    options.setCreateIfMissing(true)

    val dbDir = new File(System.getProperty("java.io.tmpdir"))
    val rocksDB = new RocksDbKeyValueStore(dbDir, options, config, false, "dbStore")

    rocksDB.close()

    val key = "key".getBytes("UTF-8")
    rocksDB.get(key)
  }

  @Test
  def testIteratorWithRemoval(): Unit = {
    val lock = new Object

    val map = new util.HashMap[String, String]()
    val config = new MapConfig(map)
    val options = new Options()
    options.setCreateIfMissing(true)
    val rocksDB = RocksDbKeyValueStore.openDB(new File(System.getProperty("java.io.tmpdir")),
                                              options,
                                              config,
                                              false,
                                              "dbStore",
                                              null)

    val key = "key".getBytes("UTF-8")
    val key1 = "key1".getBytes("UTF-8")
    val value = "val".getBytes("UTF-8")
    val value1 = "val1".getBytes("UTF-8")

    var iter: RocksIterator = null

    lock.synchronized {
      rocksDB.put(key, value)
      rocksDB.put(key1, value1)
      // SAMZA-836: Mysteriously,calling new FlushOptions() does not invoke the NativeLibraryLoader in rocksdbjni-3.13.1!
      // Moving this line after calling new Options() resolve the issue.
      val flushOptions = new FlushOptions().setWaitForFlush(true)
      rocksDB.flush(flushOptions)

      iter = rocksDB.newIterator()
      iter.seekToFirst()
    }

    while (iter.isValid) {
      iter.next()
    }
    iter.dispose()

    lock.synchronized {
      rocksDB.remove(key)
      iter = rocksDB.newIterator()
      iter.seek(key)
    }

    while (iter.isValid) {
      iter.next()
    }
    iter.dispose()

    val dbDir = new File(System.getProperty("java.io.tmpdir")).toString
    val rocksDBReadOnly = RocksDB.openReadOnly(options, dbDir)
    Assert.assertEquals(new String(rocksDBReadOnly.get(key1), "UTF-8"), "val1")
    Assert.assertEquals(rocksDBReadOnly.get(key), null)
    rocksDB.close()
    rocksDBReadOnly.close()
  }

  @Test
  def testMetricsConfig(): Unit = {
    val registry = new MetricsRegistryMap("registrymap")
    val metrics = new KeyValueStoreMetrics("dbstore", registry)

    val map = new util.HashMap[String, String]()
    map.put("rocksdb.metrics.list", "rocksdb.estimate-num-keys, rocksdb.estimate-live-data-size")
    val config = new MapConfig(map)
    val options = new Options()
    options.setCreateIfMissing(true)
    val rocksDB = RocksDbKeyValueStore.openDB(
      new File(System.getProperty("java.io.tmpdir")),
      options,
      config,
      false,
      "dbstore",
      metrics)

    val metricsGroup = registry.getGroup("org.apache.samza.storage.kv.KeyValueStoreMetrics")
    assert(metricsGroup != null)

    val estimateNumKeysMetric = metricsGroup.get("dbstore-rocksdb.estimate-num-keys")
    assert(estimateNumKeysMetric.isInstanceOf[Gauge[String]])

    val estimateLiveDataSizeMetric = metricsGroup.get("dbstore-rocksdb.estimate-live-data-size")
    assert(estimateLiveDataSizeMetric.isInstanceOf[Gauge[String]])

    rocksDB.close()
  }
}
