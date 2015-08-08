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

package org.apache.samza.test.integration

import org.apache.samza.config.Config
import org.apache.samza.storage.kv.KeyValueStore
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.task.{MessageCollector, TaskContext, TaskCoordinator}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}

import scala.collection.JavaConversions._

object TestShutdownStatefulTask {
  val STORE_NAME = "loggedstore"
  val STATE_TOPIC_STREAM = "storeChangelog"

  @BeforeClass
  def beforeSetupServers {
    StreamTaskTestUtil.beforeSetupServers
  }

  @AfterClass
  def afterCleanLogDirs {
    StreamTaskTestUtil.afterCleanLogDirs
  }
}

/**
 * Test that does the following:
 * 1. Start a single partition of TestShutdownTask using ThreadJobFactory.
 * 2. Send six messages to input (1,2,3,2,99,99) and validate that all messages were received by TestShutdownStateStoreTask.
 * 3. Shutdown the job.
 * 4. Start the job again.
 * 5. Validate that the job restored all states (1,2,3,99) to the store, including the pending flushed messages before shutdown
 */
class TestShutdownStatefulTask extends StreamTaskTestUtil {

  StreamTaskTestUtil(Map(
    "job.name" -> "state-stateful-world",
    "task.class" -> "org.apache.samza.test.integration.ShutdownStateStoreTask",
    "task.commit.ms" -> "-1",
    "stores.loggedstore.factory" -> "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory",
    "stores.loggedstore.key.serde" -> "string",
    "stores.loggedstore.msg.serde" -> "string",
    "stores.loggedstore.changelog" -> "kafka.storeChangelog",
    "stores.loggedstore.changelog.replication.factor" -> "1",
    // However, don't have the inputs use the checkpoint manager
    // since the second part of the test expects to replay the input streams.
    "systems.kafka.streams.input.samza.reset.offset" -> "false"))

  @Test
  def testShouldStartAndRestore {
    // Have to do this in one test to guarantee ordering.
    testShouldStartTaskForFirstTime
    testShouldRestoreStore
  }

  def testShouldStartTaskForFirstTime {
    val (job, task) = startJob

    // Validate that restored is empty.
    assertEquals(0, task.initFinished.getCount)
    assertEquals(0, task.asInstanceOf[ShutdownStateStoreTask].restored.size)
    assertEquals(0, task.received.size)

    // Send some messages to input stream.
    send(task, "1")
    send(task, "2")
    send(task, "3")
    send(task, "2")
    send(task, "99")
    send(task, "99")

    stopJob(job)

  }

  def testShouldRestoreStore {
    val (job, task) = startJob

    // Validate that restored has expected data.
    assertEquals(4, task.asInstanceOf[ShutdownStateStoreTask].restored.size)
    assertTrue(task.asInstanceOf[ShutdownStateStoreTask].restored.get("1").get.toInt == 1)
    assertTrue(task.asInstanceOf[ShutdownStateStoreTask].restored.get("2").get.toInt == 2)
    assertTrue(task.asInstanceOf[ShutdownStateStoreTask].restored.get("3").get.toInt == 1)
    assertTrue(task.asInstanceOf[ShutdownStateStoreTask].restored.get("99").get.toInt == 2)

    stopJob(job)
  }
}

/**
 * This ShutdownStateStoreTask implements a simple task w/ a state store that counts the occurence of msgs received
 */
class ShutdownStateStoreTask extends TestTask {
  var store: KeyValueStore[String, String] = null
  var restored = scala.collection.mutable.Map[String, String]()

  override def testInit(config: Config, context: TaskContext) {
    store = context
      .getStore(TestShutdownStatefulTask.STORE_NAME)
      .asInstanceOf[KeyValueStore[String, String]]
    val iter = store.all
    iter.foreach( p => restored += (p.getKey -> p.getValue))
    System.err.println("ShutdownStateStoreTask.init(): %s" format restored)
    iter.close
  }

  override def testProcess(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator) {
    val msg = envelope.getMessage.asInstanceOf[String]

    // Count the specific string received
    val cntStr = store.get(msg)
    if (cntStr == null) {
      store.put(msg, "1")
    } else {
      val count = cntStr.toInt + 1
      store.put(msg, count.toString)
    }
  }
}
