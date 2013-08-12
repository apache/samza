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

import org.apache.samza.serializers.SerdeManager

class SystemProducers(
  producers: Map[String, SystemProducer],
  serdeManager: SerdeManager) {

  // TODO add metrics and logging

  def start {
    producers.values.foreach(_.start)
  }

  def stop {
    producers.values.foreach(_.stop)
  }

  def register(source: String) {
    producers.values.foreach(_.register(source))
  }

  def commit(source: String) {
    producers.values.foreach(_.commit(source))
  }

  def send(source: String, envelope: OutgoingMessageEnvelope) {
    producers(envelope.getSystemStream.getSystem).send(source, serdeManager.toBytes(envelope))
  }
}
