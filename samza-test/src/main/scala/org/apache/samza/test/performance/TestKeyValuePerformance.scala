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

import java.io.File
import java.util
import java.util.UUID
import java.util.concurrent.TimeUnit

import com.google.common.base.Stopwatch
import org.apache.samza.config.Config
import org.apache.samza.config.StorageConfig._
import org.apache.samza.container.{SamzaContainerContext, TaskName}
import org.apache.samza.metrics.MetricsRegistryMap
import org.apache.samza.serializers.{ByteSerde, SerdeManager, UUIDSerde}
import org.apache.samza.storage.StorageEngineFactory
import org.apache.samza.storage.kv.{KeyValueStorageEngine, KeyValueStore}
import org.apache.samza.system.{SystemProducer, SystemProducers}
import org.apache.samza.task.TaskInstanceCollector
import org.apache.samza.util.{CommandLine, Logging, Util}
import org.apache.samza.{Partition, SamzaException}

import scala.collection.JavaConversions._
import scala.util.Random

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
    "rocksdb-write-performance" -> runTestMsgWritePerformance,
    "get-all-vs-get-write-many-read-many" -> runTestGetAllVsGetWriteManyReadMany,
    "get-all-vs-get-write-once-read-many" -> runTestGetAllVsGetWriteOnceReadMany)

  def main(args: Array[String]) {
    val cmdline = new CommandLine
    val options = cmdline.parser.parse(args: _*)
    val config = cmdline.loadConfig(options)
    val tests = config.get("test.methods").split(",")

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
    val taskNames = new java.util.ArrayList[TaskName]()
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

  def runTestGetAllVsGetWriteManyReadMany(db: KeyValueStore[Array[Byte], Array[Byte]], config: Config) {
    new TestKeyValuePerformance().testGetAllVsGetWriteManyReadMany(db, config)
  }

  def runTestGetAllVsGetWriteOnceReadMany(db: KeyValueStore[Array[Byte], Array[Byte]], config: Config) {
    new TestKeyValuePerformance().testGetAllVsGetWriteOnceReadMany(db, config)
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

  /**
   * Test that ::getAll performance is better than that of ::get (test when there are many writes and many reads).
   * @param store key-value store instance that is being tested
   * @param config the test case's config
   */
  def testGetAllVsGetWriteManyReadMany(store: KeyValueStore[Array[Byte],Array[Byte]], config: Config): Unit = {
    val iterationsCount = config.getInt("iterations.count", 100)
    val maxMessagesCountPerBatch = config.getInt("message.max-count-per-batch", 100000)
    val maxMessageSizeBytes = config.getInt("message.max-size.bytes", 1024)
    val timer = Stopwatch.createUnstarted
    val uuidSerde = new UUIDSerde

    info("iterations count: " + iterationsCount)
    info("max messages count per batch: " + maxMessagesCountPerBatch)
    info("max message size in bytes: " + maxMessageSizeBytes)
    info("%12s%12s%12s%12s".format("Msg Count", "Bytes/Msg", "get ms", "getAll ms"))

    try {
      (0 until iterationsCount).foreach(i => {
        val messageSizeBytes = Random.nextInt(maxMessageSizeBytes)
        val messagesCountPerBatch = Random.nextInt(maxMessagesCountPerBatch)
        val keys = (0 until messagesCountPerBatch).map(k => uuidSerde.toBytes(UUID.randomUUID)).toList
        val shuffledKeys = Random.shuffle(keys) // to reduce locality of reference -- sequential access may be unfair

        keys.foreach(k => store.put(k, Random.nextString(messageSizeBytes).getBytes(Encoding)))
        store.flush()

        timer.reset().start()
        assert(store.getAll(shuffledKeys).size == shuffledKeys.size)
        val getAllTime = timer.stop().elapsed(TimeUnit.MILLISECONDS)

        // Restore cache, in case it's enabled, to a state similar to the one above when the getAll test started
        keys.foreach(k => store.put(k, Random.nextString(messageSizeBytes).getBytes(Encoding)))
        store.flush()

        timer.reset().start()
        shuffledKeys.foreach(store.get)
        val getTime = timer.stop().elapsed(TimeUnit.MILLISECONDS)

        info("%12d%12d%12d%12d".format(messagesCountPerBatch, messageSizeBytes, getTime, getAllTime))
        if (getAllTime > getTime) {
          error("getAll was slower than get!")
        }
      })
    } finally {
      store.close()
    }
  }

  /**
   * Test that ::getAll performance is better than that of ::get (test when data are written once and read many times);
   * load is usually greater than the storage engine's cache size (not to be confused with Samza's cache layer),
   * and keys are randomly selected from the stored entries to perform a fair comparison of ::get vs. ::getAll (in case
   * the underlying storage engine caches data in blocks and ::getAll causes a block to be loaded into the cache --
   * one can argue that ::get should trigger the same behavior, but it's worth testing this WORM scenario regardless)
   * @param store key-value store instance that is being tested
   * @param config the test case's config
   */
  def testGetAllVsGetWriteOnceReadMany(store: KeyValueStore[Array[Byte],Array[Byte]], config: Config): Unit = {
    val iterationsCount = config.getInt("iterations.count", 100)
    val maxMessagesCountPerBatch = config.getInt("message.max-count-per-batch", 10000 + Random.nextInt(20000))
    val maxMessageSizeBytes = config.getInt("message.max-size.bytes", 1024)
    val totalMessagesCount = iterationsCount * maxMessagesCountPerBatch
    val timer = Stopwatch.createUnstarted
    val uuidSerde = new UUIDSerde

    info("write once -- putting %d messages in store".format(totalMessagesCount))
    val keys = (0 until totalMessagesCount).map(k => uuidSerde.toBytes(UUID.randomUUID)).toList
    keys.foreach(k => store.put(k, Random.nextString(Random.nextInt(maxMessageSizeBytes)).getBytes(Encoding)))
    store.flush()

    info("iterations count: " + iterationsCount)
    info("max messages count per batch: " + maxMessagesCountPerBatch)
    info("max message size in bytes: " + maxMessageSizeBytes)
    info("%12s%12s%12s%12s".format("Msg Count", "Total Size", "get ms", "getAll ms"))

    try {
      (0 until iterationsCount).foreach(i => {
        val messagesCountPerBatch = Random.nextInt(maxMessagesCountPerBatch)
        val shuffledKeys = Random.shuffle(keys).take(messagesCountPerBatch)

        // We want to measure ::getAll when called many times, so populate the cache because first call is a cache-miss
        val totalSize = store.getAll(shuffledKeys).values.map(_.length).sum
        timer.reset().start()
        assert(store.getAll(shuffledKeys).size == shuffledKeys.size)
        val getAllTime = timer.stop().elapsed(TimeUnit.MILLISECONDS)

        // We want to measure ::get when called many times, so populate the cache because first call is a cache-miss
        shuffledKeys.foreach(store.get)
        timer.reset().start()
        shuffledKeys.foreach(store.get)
        val getTime = timer.stop().elapsed(TimeUnit.MILLISECONDS)

        info("%12d%12d%12d%12d".format(messagesCountPerBatch, totalSize, getTime, getAllTime))
        if (getAllTime > getTime) {
          error("getAll was slower than get!")
        }
      })
    } finally {
      store.close()
    }
  }
}
