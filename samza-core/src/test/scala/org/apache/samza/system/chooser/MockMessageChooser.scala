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

package org.apache.samza.system.chooser

import org.apache.samza.system.IncomingMessageEnvelope
import org.apache.samza.system.SystemStreamPartition

class MockMessageChooser extends MessageChooser {
  var envelopes = scala.collection.mutable.Queue[IncomingMessageEnvelope]()
  var starts = 0
  var stops = 0
  var registers = Map[SystemStreamPartition, String]()

  def start = starts += 1

  def stop = stops += 1

  def register(systemStreamPartition: SystemStreamPartition, lastReadOffset: String) = registers += systemStreamPartition -> lastReadOffset

  def update(envelope: IncomingMessageEnvelope) {
    envelopes += envelope
  }

  def choose = {
    try {
      envelopes.dequeue
    } catch {
      case e: NoSuchElementException => null
    }
  }

  def getEnvelopes = envelopes
}