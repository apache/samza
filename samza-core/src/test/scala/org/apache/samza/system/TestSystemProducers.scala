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

import org.junit.Assert._
import org.junit.Test
import org.apache.samza.serializers._
import org.apache.samza.SamzaException

class TestSystemProducers {

  @Test
  def testDroppingMsgOrThrowExceptionWhenSerdeFails() {
    val system = "test-system"
    val systemStream = new SystemStream(system, "stream1")
    val systemMessageSerdes = Map(system -> (new StringSerde("UTF-8")).asInstanceOf[Serde[Object]]);
    val serdeManager = new SerdeManager(systemMessageSerdes = systemMessageSerdes)
    val systemProducer = new SystemProducer {
      def start {}
      def stop {}
      def register(source: String) {}
      def send(source: String, envelope: OutgoingMessageEnvelope) {}
      def flush(source: String) {}
    }
    val systemProducers = new SystemProducers(Map(system -> systemProducer), serdeManager, new SystemProducersMetrics, false)
    systemProducers.register(system)
    val outgoingEnvelopeCorrectMsg = new OutgoingMessageEnvelope (systemStream, "test")
    val outgoingEnvelopeErrorMsg = new OutgoingMessageEnvelope (systemStream, 123)
    systemProducers.send(system, outgoingEnvelopeCorrectMsg)

    var getCorrectException = false
    try {
      systemProducers.send(system, outgoingEnvelopeErrorMsg)
    } catch {
      case e: SamzaException => getCorrectException = true
      case _: Throwable => getCorrectException = false
    }
    assertTrue(getCorrectException)

    val systemProducers2 = new SystemProducers(Map(system -> systemProducer), serdeManager, new SystemProducersMetrics, true)
    systemProducers2.register(system)
    systemProducers2.send(system, outgoingEnvelopeCorrectMsg)

    var notThrowException = true
    try {
      systemProducers2.send(system, outgoingEnvelopeErrorMsg)
    } catch {
      case _: Throwable => notThrowException = false
    }
    assertTrue(notThrowException)
  }
}