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

import org.apache.samza.job.local.ThreadJobFactory
import org.junit.Test
import org.apache.samza.task.StreamTask
import org.apache.samza.task.TaskCoordinator
import org.apache.samza.task.MessageCollector
import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.config.MapConfig
import scala.collection.JavaConversions._
import org.apache.samza.job.ShellCommandBuilder
import org.apache.samza.task.InitableTask
import org.apache.samza.task.TaskContext
import org.apache.samza.config.Config
import grizzled.slf4j.Logging

/**
 * A simple unit test that drives the TestPerformanceTask. This unit test can
 * be triggered by itself using:
 *
 * <pre>
 * ./gradlew :samza-test:test -Dtest.single=TestSamzaContainerPerformance
 * <pre>
 *
 * Once the test is running, you can attach JConsole, VisualVM, or YourKit to
 * have a look at how things are behaving.
 *
 * The test can be configured with the following system properties:
 *
 * <pre>
 * samza.mock.consumer.thread.count
 * samza.mock.messages.per.batch
 * samza.mock.input.streams
 * samza.mock.partitions.per.stream
 * samza.mock.broker.sleep.ms
 * samza.task.log.interval
 * samza.task.max.messages
 * <pre>
 *
 * For example, you might specify wish to process 10000 messages simulated 
 * from two input streams on one broker:
 *
 * <pre>
 * ./gradlew :samza-test:test \
 *   -Dsamza.test.single=TestSamzaContainerPerformance \
 *   -Psamza.mock.input.streams=2 \
 *   -Psamza.mock.consumer.thread.count=1 \
 *   -Psamza.task.log.interval=1000 \
 *   -Psamza.task.max.messages=10000
 * <pre>
 */
class TestSamzaContainerPerformance extends Logging{
  val consumerThreadCount = System.getProperty("samza.mock.consumer.thread.count", "12").toInt
  val messagesPerBatch = System.getProperty("samza.mock.messages.per.batch", "5000").toInt
  val streamCount = System.getProperty("samza.mock.input.streams", "1000").toInt
  val partitionsPerStreamCount = System.getProperty("samza.mock.partitions.per.stream", "4").toInt
  val brokerSleepMs = System.getProperty("samza.mock.broker.sleep.ms", "1").toInt
  var logInterval = System.getProperty("samza.task.log.interval", "10000").toInt
  var maxMessages = System.getProperty("samza.task.max.messages", "10000000").toInt

  val jobFactory = new ThreadJobFactory

  val jobConfig = Map(
    "job.factory.class" -> jobFactory.getClass.getCanonicalName,
    "job.name" -> "test-container-performance",
    "task.class" -> classOf[TestPerformanceTask].getName,
    "task.inputs" -> (0 until streamCount).map(i => "mock.stream" + i).mkString(","),
    "task.log.interval" -> logInterval.toString,
    "task.max.messages" -> maxMessages.toString,
    "systems.mock.samza.factory" -> classOf[org.apache.samza.system.mock.MockSystemFactory].getCanonicalName,
    "systems.mock.partitions.per.stream" -> partitionsPerStreamCount.toString,
    "systems.mock.messages.per.batch" -> messagesPerBatch.toString,
    "systems.mock.consumer.thread.count" -> consumerThreadCount.toString,
    "systems.mock.broker.sleep.ms" -> brokerSleepMs.toString)

  @Test
  def testContainerPerformance {
    info("Testing performance with configuration: %s" format jobConfig)

    val job = jobFactory
      .getJob(new MapConfig(jobConfig))
      .submit

    job.waitForFinish(Int.MaxValue)
  }
}
