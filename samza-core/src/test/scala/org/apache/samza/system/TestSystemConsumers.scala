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

package org.apache.samza.system

import scala.collection.JavaConversions._
import org.apache.samza.Partition
import org.junit.Assert._
import org.junit.Test
import org.apache.samza.system.chooser.MessageChooser

class TestSystemConsumers {
  @Test
  def testSystemConumersShouldRegisterStartAndStopChooser {
    val system = "test-system"
    val systemStreamPartition = new SystemStreamPartition(system, "some-stream", new Partition(1))
    var started = 0
    var stopped = 0
    var registered = Map[SystemStreamPartition, String]()

    val consumer = Map(system -> new SystemConsumer {
      def start {}
      def stop {}
      def register(systemStreamPartition: SystemStreamPartition, offset: String) {}
      def poll(systemStreamPartitions: java.util.Map[SystemStreamPartition, java.lang.Integer], timeout: Long) = List()
    })

    val consumers = new SystemConsumers(new MessageChooser {
      def update(envelope: IncomingMessageEnvelope) = Unit
      def choose = null
      def start = started += 1
      def stop = stopped += 1
      def register(systemStreamPartition: SystemStreamPartition, offset: String) = registered += systemStreamPartition -> offset
    }, consumer, null)

    consumers.register(systemStreamPartition, "0")
    consumers.start
    consumers.stop

    assertEquals(1, started)
    assertEquals(1, stopped)
    assertEquals(1, registered.size)
    assertEquals("0", registered(systemStreamPartition))
  }

  @Test
  def testThrowSystemConsumersExceptionWhenTheSystemDoesNotHaveConsumer() {
    val system = "test-system"
    val system2 = "test-system2"
    val systemStreamPartition = new SystemStreamPartition(system, "some-stream", new Partition(1))
    val systemStreamPartition2 = new SystemStreamPartition(system2, "some-stream", new Partition(1))
    var started = 0
    var stopped = 0
    var registered = Map[SystemStreamPartition, String]()
    
    val consumer = Map(system -> new SystemConsumer {
      def start {}
      def stop {}
      def register(systemStreamPartition: SystemStreamPartition, offset: String) {}
      def poll(systemStreamPartitions: java.util.Map[SystemStreamPartition, java.lang.Integer], timeout: Long) = List()
    })
    val consumers = new SystemConsumers(new MessageChooser {
      def update(envelope: IncomingMessageEnvelope) = Unit
      def choose = null
      def start = started += 1
      def stop = stopped += 1
      def register(systemStreamPartition: SystemStreamPartition, offset: String) = registered += systemStreamPartition -> offset
    }, consumer, null)
     
    // it should throw a SystemConsumersException because system2 does not have a consumer
    var caughtRightException = false
    try {
    	consumers.register(systemStreamPartition2, "0")
    } catch {
    	case e: SystemConsumersException => caughtRightException = true
    	case _: Throwable => caughtRightException = false
    }
    assertTrue("suppose to throw SystemConsumersException, but apparently it did not", caughtRightException)
  }
}