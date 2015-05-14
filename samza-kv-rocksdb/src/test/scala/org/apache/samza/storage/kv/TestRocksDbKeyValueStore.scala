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

import org.apache.samza.{SamzaException, Partition}
import org.apache.samza.config.MapConfig
import org.apache.samza.container.{TaskName, SamzaContainerContext}
import org.apache.samza.system.SystemStreamPartition
import org.apache.samza.util.{NoOpMetricsRegistry, ExponentialSleepStrategy}
import org.apache.samza.util.Util._
import org.junit.{Assert, Test}
import org.rocksdb.{RocksDBException, Options}

class TestRocksDbKeyValueStore
{
  @Test
  def testTTL() {
    val map = new java.util.HashMap[String, String]();
    map.put("rocksdb.ttl.ms", "1000")
    val config = new MapConfig(map)
    val options = new Options()
    options.setCreateIfMissing(true)
    val rocksDB = RocksDbKeyValueStore.openDB(new File(System.getProperty("java.io.tmpdir")),
                                              options,
                                              config,
                                              false,
                                              "someStore")
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
}
