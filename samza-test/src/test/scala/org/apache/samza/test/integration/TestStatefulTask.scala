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

import scala.collection.JavaConverters._

object TestStatefulTask {
  val STORE_NAME = "mystore"
  val STATE_TOPIC_STREAM = "mystoreChangelog"

  // Messages with one dupe and one delete. A negative string means delete. See StateStoreTestTask.testProcess()
  val MESSAGES_SEND_1 = List("1", "2", "3", "2", "99", "-99")
  val MESSAGES_RECV_1 = List("1", "2", "3", "2", "99", null)
  val STORE_CONTENTS_1 = List("1", "2", "3")

  val MESSAGES_SEND_2 = List("4", "5", "5")
  val MESSAGES_RECV_2 = List("4", "5", "5")

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
 * 2. Send MESSAGES_SEND_1, which contains a dupe and a delete.
 * 3. Validate that all messages were received by TestStateStoreTask.
 * 4. Validate that TestStateStoreTask called store.put() for all messages, and that the messages ended up in the mystore topic.
 * 5. Kill the job.
 * 6. Start the job again.
 * 7. Validate that the job restored all messages STORE_CONTENTS_1 to the store.
 * 8. Send three more messages to input MESSAGES_SEND_2, and validate that TestStateStoreTask receives them.
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
    TestStatefulTask.MESSAGES_SEND_1.foreach(m => send(task, m))

    // Validate that messages appear in store stream.
    val messages = readAll(TestStatefulTask.STATE_TOPIC_STREAM, TestStatefulTask.MESSAGES_RECV_1.length-1, "testShouldStartTaskForFirstTime")

    assertEquals(TestStatefulTask.MESSAGES_RECV_1, messages)

    stopJob(job)
  }

  def testShouldRestoreStore {
    val (job, task) = startJob

    // Validate that restored has expected data.
    assertEquals(TestStatefulTask.STORE_CONTENTS_1.length, task.asInstanceOf[StateStoreTestTask].restored.size)
    TestStatefulTask.STORE_CONTENTS_1.foreach(m =>  assertTrue(task.asInstanceOf[StateStoreTestTask].restored.contains(m)))

    var count = 0

    // We should get the original size messages in the stream (1,2,3,2,99,-99).
    // Note that this will trigger four new outgoing messages to the STATE_TOPIC.
    while (task.received.size < TestStatefulTask.MESSAGES_RECV_1.length && count < 100) {
      Thread.sleep(600)
      count += 1
    }

    assertTrue("Timed out waiting to received messages. Received thus far: " + task.received.size, count < 100)

    // Reset the count down latch after the 6 messages come in.
    task.awaitMessage

    // Send some messages to input stream.
    TestStatefulTask.MESSAGES_SEND_2.foreach(m => send(task, m))

    val expectedMessagesRcvd =  TestStatefulTask.MESSAGES_RECV_1 ++ // From initial start.
                                TestStatefulTask.MESSAGES_RECV_1 ++ // From second startup.
                                TestStatefulTask.MESSAGES_RECV_2    // From sending in this method.

    // Validate that messages appear in store stream.
    val messages = readAll(TestStatefulTask.STATE_TOPIC_STREAM, expectedMessagesRcvd.length-1, "testShouldRestoreStore")

    assertEquals(expectedMessagesRcvd, messages)

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
      .asScala
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

