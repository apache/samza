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

package org.apache.samza.test.performance

import org.apache.samza.util.Logging
import org.apache.samza.config.Config
import org.apache.samza.config.StorageConfig._
import org.apache.samza.container.{ TaskName, SamzaContainerContext }
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.storage.kv.KeyValueStorageEngine
import org.apache.samza.storage.StorageEngineFactory
import org.apache.samza.util.CommandLine
import org.apache.samza.util.Util
import org.apache.samza.serializers.ByteSerde
import org.apache.samza.Partition
import org.apache.samza.SamzaException
import org.apache.samza.task.TaskInstanceCollector
import org.apache.samza.system.SystemProducers
import org.apache.samza.system.SystemProducer
import org.apache.samza.serializers.SerdeManager
import java.io.File
import java.util.UUID

/**
 * A simple CLI-based tool for running various key-value performance tests.
 */
object TestKeyValuePerformance extends Logging {
  val Encoding = "UTF-8"

  /**
   * KeyValuePerformance job configs must define a 'test.method' configuration.
   * This configuration must be the value of one of the keys in this map. The
   * test uses this key to determine which test to run.
   */
  val testMethods: Map[String, (Config) => Unit] = Map("testAllWithDeletes" -> runTestAllWithDeletes)

  def main(args: Array[String]) {
    val cmdline = new CommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    val testMethod = config.get("test.method", "testAllWithDeletes")

    info("Got arguments: %s" format args.toList)
    info("Using config: %s" format config)
    info("Using test method: %s" format testMethod)

    if (testMethods.contains(testMethod)) {
      testMethods(testMethod)(config)
    } else {
      error("Invalid test method. Valid methods are: %s" format testMethods.keys)

      throw new SamzaException("Unknown test method: %s" format testMethod)
    }
  }

  /**
   * Do wiring for testAllWithDeletes, and run the test.
   */
  def runTestAllWithDeletes(config: Config) {
    val test = new TestKeyValuePerformance
    val serde = new ByteSerde
    val partitionCount = config.getInt("test.partition.count", 1)
    val numLoops = config.getInt("test.num.loops", 100)
    val messagesPerBatch = config.getInt("test.messages.per.batch", 10000)
    val messageSizeBytes = config.getInt("test.message.size.bytes", 200)
    val taskNames = new java.util.ArrayList[TaskName]()

    (0 until partitionCount).map(p => taskNames.add(new TaskName(new Partition(p).toString)))

    info("Using partition count: %s" format partitionCount)
    info("Using num loops: %s" format numLoops)
    info("Using messages per batch: %s" format messagesPerBatch)
    info("Using message size: %s bytes" format messageSizeBytes)

    // Build a Map[String, StorageEngineFactory]. The key is the store name.
    val storageEngineFactories = config
      .getStoreNames
      .map(storeName => {
        val storageFactoryClassName = config
          .getStorageFactoryClassName(storeName)
          .getOrElse(throw new SamzaException("Missing storage factory for %s." format storeName))
        (storeName, Util.getObj[StorageEngineFactory[Array[Byte], Array[Byte]]](storageFactoryClassName))
      }).toMap

    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager)

    for ((storeName, storageEngine) <- storageEngineFactories) {
      val output = new File("/tmp/" + UUID.randomUUID)

      info("Using output directory %s for %s." format (output, storeName))

      val engine = storageEngine.getStorageEngine(
        storeName,
        output,
        serde,
        serde,
        new TaskInstanceCollector(producerMultiplexer),
        new MetricsRegistryMap,
        null,
        new SamzaContainerContext("test", config, taskNames))

      val db = if (!engine.isInstanceOf[KeyValueStorageEngine[_, _]]) {
        throw new SamzaException("This test can only run with KeyValueStorageEngine configured as store factory.")
      } else {
        engine.asInstanceOf[KeyValueStorageEngine[Array[Byte], Array[Byte]]]
      }

      test.testAllWithDeletes(db, numLoops, messagesPerBatch, messageSizeBytes)
      info("Cleaning up output directory for %s." format storeName)
      Util.rm(output)
    }
  }
}

class TestKeyValuePerformance extends Logging {
  import TestKeyValuePerformance._

  /**
   * A test that writes messagesPerBatch messages, deletes them all, then calls
   * store.all. The test periodically outputs the time it takes to complete
   * these operations. This test is useful to trouble shoot issues with LevleDB
   * such as the issue documented in SAMZA-254.
   */
  def testAllWithDeletes(
    store: KeyValueStore[Array[Byte], Array[Byte]],

    /**
     * How many times a batch of messages should be written and deleted.
     */
    numLoops: Int = 100,

    /**
     * The number of messages to write and delete per-batch.
     */
    messagesPerBatch: Int = 10000,

    /**
     * The size of the messages to write.
     */
    messageSizeBytes: Int = 200) {

    val stuff = (0 until messageSizeBytes).map(i => "a").mkString.getBytes(Encoding)
    val start = System.currentTimeMillis

    (0 until numLoops).foreach(i => {
      info("(%sms) Total written to store: %s" format (System.currentTimeMillis - start, i * messagesPerBatch))

      (0 until messagesPerBatch).foreach(j => {
        val k = (i * j).toString.getBytes(Encoding)
        store.put(k, stuff)
        store.delete(k)
      })

      val allStart = System.currentTimeMillis
      val iter = store.all
      info("(%sms) all() took %sms." format (System.currentTimeMillis - start, System.currentTimeMillis - allStart))
      iter.close
    })

    info("Total time: %ss" format ((System.currentTimeMillis - start) * .001))
  }
}