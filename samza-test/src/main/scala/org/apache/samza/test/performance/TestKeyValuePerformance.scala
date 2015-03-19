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
import org.apache.samza.container.{TaskName, SamzaContainerContext}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.storage.kv.KeyValueStorageEngine
import org.apache.samza.storage.StorageEngineFactory
import org.apache.samza.util.CommandLine
import org.apache.samza.util.Util
import org.apache.samza.serializers.{StringSerde, ByteSerde, SerdeManager}
import org.apache.samza.Partition
import org.apache.samza.SamzaException
import org.apache.samza.task.TaskInstanceCollector
import org.apache.samza.system.SystemProducers
import org.apache.samza.system.SystemProducer
import java.io.File
import java.util.UUID
import java.util

/**
 * A simple CLI-based tool for running various key-value performance tests.
 *
 * List of KeyValuePerformance tests must be defined in 'test.methods' configuration as a comma-separated value.
 * The tool splits this list to determine which tests to run.
 *
 * Each test should define its own set of configuration for partition count, stores etc.
 * using the "test.<test-name>.<config-string>=<config-value>" pattern
 *
 * Each test may define one or more test parameterss.
 * For example, test1 can define 2 sets of parameters by specifying "test.test1.set.count=2" and
 * define each set as:
 * "test.test1.set-1.<param-name>=<param-value>"
 * "test.test1.set-2.<param-name>=<param-value>"
 */

object TestKeyValuePerformance extends Logging {
  val Encoding = "UTF-8"

  val testMethods: Map[String, (KeyValueStorageEngine[Array[Byte], Array[Byte]], Config) => Unit] = Map(
    "all-with-deletes" -> runTestAllWithDeletes,
    "rocksdb-write-performance" -> runTestMsgWritePerformance
  )

  def main(args: Array[String]) {
    val cmdline = new CommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    val tests = config.get("test.methods", "rocksdb-write-performance,all-with-deletes").split(",")

    tests.foreach{ test =>
      info("Running test: %s" format test)
      if(testMethods.contains(test)) {
        invokeTest(test, testMethods(test), config.subset("test." + test + ".", true))
      } else {
        error("Invalid test method. valid methods are: %s" format testMethods.keys)
        throw new SamzaException("Unknown test method: %s" format test)
      }
    }
  }

  def invokeTest(testName: String, testMethod: (KeyValueStorageEngine[Array[Byte], Array[Byte]], Config) => Unit, config: Config) {
    val taskNames = new util.ArrayList[TaskName]()
    val partitionCount = config.getInt("partition.count", 1)
    (0 until partitionCount).map(p => taskNames.add(new TaskName(new Partition(p).toString)))

    val producerMultiplexer = new SystemProducers(
      Map[String, SystemProducer](),
      new SerdeManager
    )
    // Build a Map[String, StorageEngineFactory]. The key is the store name.
    val storageEngineMappings = config
      .getStoreNames
      .map(storeName => {
        val storageFactoryClassName =
          config.getStorageFactoryClassName(storeName)
                .getOrElse(throw new SamzaException("Missing storage factory for %s." format storeName))
        (storeName, Util.getObj[StorageEngineFactory[Array[Byte], Array[Byte]]](storageFactoryClassName))
    })

    for((storeName, storageEngine) <- storageEngineMappings) {
      val testSetCount = config.getInt("set.count", 1)
      (1 to testSetCount).foreach(testSet => {
        //Create a new DB instance for each test set
        val output = new File("/tmp/" + UUID.randomUUID())
        val byteSerde = new ByteSerde
        info("Using output directory %s for %s using %s." format (output, storeName, storageEngine.getClass.getCanonicalName))
        val engine = storageEngine.getStorageEngine(
          storeName,
          output,
          byteSerde,
          byteSerde,
          new TaskInstanceCollector(producerMultiplexer),
          new MetricsRegistryMap,
          null,
          new SamzaContainerContext(0, config, taskNames)
        )

        val db = if(!engine.isInstanceOf[KeyValueStorageEngine[_,_]]) {
          throw new SamzaException("This test can only run with KeyValueStorageEngine configured as store factory.")
        } else {
          engine.asInstanceOf[KeyValueStorageEngine[Array[Byte], Array[Byte]]]
        }

        // Run the test method
        testMethod(db, config.subset("set-" + testSet + ".", true))

        info("Cleaning up output directory for %s." format storeName)
        Util.rm(output)
      })
    }
  }

  def runTestAllWithDeletes(db: KeyValueStore[Array[Byte], Array[Byte]], config: Config) {
    val numLoops = config.getInt("num.loops", 100)
    val messagesPerBatch = config.getInt("messages.per.batch", 10000)
    val messageSizeBytes = config.getInt("message.size.bytes", 200)

    info("Using (num loops, messages per batch, message size in bytes) => (%s, %s, %s)" format (numLoops, messagesPerBatch, messageSizeBytes))
    new TestKeyValuePerformance().testAllWithDeletes(db, numLoops, messagesPerBatch, messageSizeBytes)

  }

  def runTestMsgWritePerformance(db: KeyValueStore[Array[Byte], Array[Byte]], config: Config) {
    val messageSizeBytes = config.getInt("message.size", 200)
    val messageCount = config.getInt("message.count", 10000)

    info("Using (message count, message size in bytes) => (%s, %s)" format (messageCount, messageSizeBytes))
    new TestKeyValuePerformance().testMsgWritePerformance(db, messageCount, messageSizeBytes)
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


  /**
   * Test that successively writes a set of fixed-size messages to the KV store
   * and computes the total time for the operations
   * @param store Key-Value store instance that is being tested
   * @param numMsgs Total number of messages to write to the store
   * @param msgSizeInBytes Size of each message in Bytes
   */
  def testMsgWritePerformance(
    store: KeyValueStore[Array[Byte], Array[Byte]],
    numMsgs: Int = 10000,
    msgSizeInBytes: Int = 200) {

    val msg = (0 until msgSizeInBytes).map(i => "x").mkString.getBytes(Encoding)

    val start = System.currentTimeMillis
    (0 until numMsgs).foreach(i => {
      store.put(i.toString.getBytes(Encoding), msg)
    })
    val timeTaken = System.currentTimeMillis - start
    info("Total time to write %d msgs of size %d bytes : %s s" format (numMsgs, msgSizeInBytes, timeTaken * .001))
  }
}