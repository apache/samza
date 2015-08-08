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
import org.apache.samza.task.TaskCoordinator.RequestScope
import org.apache.samza.task.{MessageCollector, TaskContext, TaskCoordinator}
import org.junit.Assert._
import org.junit.{AfterClass, BeforeClass, Test}

import scala.collection.JavaConversions._

object TestStatefulTask {
    val STORE_NAME = "mystore"
    val STATE_TOPIC_STREAM = "mystoreChangelog"

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
 * 1. Start a single partition of TestStateStoreTask using ThreadJobFactory.
 * 2. Send four messages to input (1,2,3,2), which contain one dupe (2).
 * 3. Validate that all messages were received by TestStateStoreTask.
 * 4. Validate that TestStateStoreTask called store.put() for all four messages, and that the messages ended up in the mystore topic.
 * 5. Kill the job.
 * 6. Start the job again.
 * 7. Validate that the job restored all messages (1,2,3) to the store.
 * 8. Send three more messages to input (4,5,5), and validate that TestStateStoreTask receives them.
 * 9. Kill the job again.
 */
class TestStatefulTask extends StreamTaskTestUtil {

  StreamTaskTestUtil(Map(
    "job.name" -> "hello-stateful-world",
    "task.class" -> "org.apache.samza.test.integration.StateStoreTestTask",
    "stores.mystore.factory" -> "org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory",
    "stores.mystore.key.serde" -> "string",
    "stores.mystore.msg.serde" -> "string",
    "stores.mystore.changelog" -> "kafka.mystoreChangelog",
    "stores.mystore.changelog.replication.factor" -> "1",
    // However, don't have the inputs use the checkpoint manager
    // since the second part of the test expects to replay the input streams.
    "systems.kafka.streams.input.samza.reset.offset" -> "true"))

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
    assertEquals(0, task.asInstanceOf[StateStoreTestTask].restored.size)
    assertEquals(0, task.received.size)

    // Send some messages to input stream.
    send(task, "1")
    send(task, "2")
    send(task, "3")
    send(task, "2")
    send(task, "99")
    send(task, "-99")

    // Validate that messages appear in store stream.
    val messages = readAll(TestStatefulTask.STATE_TOPIC_STREAM, 5, "testShouldStartTaskForFirstTime")

    assertEquals(6, messages.length)
    assertEquals("1", messages(0))
    assertEquals("2", messages(1))
    assertEquals("3", messages(2))
    assertEquals("2", messages(3))
    assertEquals("99", messages(4))
    assertNull(messages(5))

    stopJob(job)
  }

  def testShouldRestoreStore {
    val (job, task) = startJob

    // Validate that restored has expected data.
    assertEquals(3, task.asInstanceOf[StateStoreTestTask].restored.size)
    assertTrue(task.asInstanceOf[StateStoreTestTask].restored.contains("1"))
    assertTrue(task.asInstanceOf[StateStoreTestTask].restored.contains("2"))
    assertTrue(task.asInstanceOf[StateStoreTestTask].restored.contains("3"))

    var count = 0

    // We should get the original four messages in the stream (1,2,3,2).
    // Note that this will trigger four new outgoing messages to the STATE_TOPIC.
    while (task.received.size < 4 && count < 100) {
      Thread.sleep(600)
      count += 1
    }

    assertTrue("Timed out waiting to received messages. Received thus far: " + task.received.size, count < 100)

    // Reset the count down latch after the 4 messages come in.
    task.awaitMessage

    // Send some messages to input stream.
    send(task, "4")
    send(task, "5")
    send(task, "5")

    // Validate that messages appear in store stream.
    val messages = readAll(TestStatefulTask.STATE_TOPIC_STREAM, 14, "testShouldRestoreStore")

    assertEquals(15, messages.length)
    // From initial start.
    assertEquals("1", messages(0))
    assertEquals("2", messages(1))
    assertEquals("3", messages(2))
    assertEquals("2", messages(3))
    assertEquals("99", messages(4))
    assertNull(messages(5))
    // From second startup.
    assertEquals("1", messages(6))
    assertEquals("2", messages(7))
    assertEquals("3", messages(8))
    assertEquals("2", messages(9))
    assertEquals("99", messages(10))
    assertNull(messages(11))
    // From sending in this method.
    assertEquals("4", messages(12))
    assertEquals("5", messages(13))
    assertEquals("5", messages(14))

    stopJob(job)
  }

}

class StateStoreTestTask extends TestTask {
  var store: KeyValueStore[String, String] = null
  var restored = Set[String]()

  override def testInit(config: Config, context: TaskContext): Unit = {
    store = context.getStore(TestStatefulTask.STORE_NAME).asInstanceOf[KeyValueStore[String, String]]
    val iter = store.all
    restored ++= iter
      .map(_.getValue)
      .toSet
    System.err.println("StateStoreTestTask.init(): %s" format restored)
    iter.close
  }

  override def testProcess(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    val msg = envelope.getMessage.asInstanceOf[String]

    // A negative string means delete
    if (msg.startsWith("-")) {
      store.delete(msg.substring(1))
    } else {
      store.put(msg, msg)
    }

    coordinator.commit(RequestScope.ALL_TASKS_IN_CONTAINER)
  }
}

