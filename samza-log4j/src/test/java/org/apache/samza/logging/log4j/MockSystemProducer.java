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

package org.apache.samza.logging.log4j;

import java.util.ArrayList;

import java.util.List;
import org.apache.log4j.Logger;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;

public class MockSystemProducer implements SystemProducer {
  public static ArrayList<Object> messagesReceived = new ArrayList<>();
  private static Logger log = Logger.getLogger(MockSystemProducer.class);
  public static List<MockSystemProducerListener> listeners = new ArrayList<>();

  @Override
  public void start() {
    log.info("mock system producer is started...");
  }

  @Override
  public void stop() {
  }

  @Override
  public void register(String source) {
  }

  @Override
  public void send(String source, OutgoingMessageEnvelope envelope) {
    messagesReceived.add(envelope.getMessage());

    listeners.forEach((listener) -> listener.onSend(source, envelope));
  }

  @Override
  public void flush(String source) {
  }

  public interface MockSystemProducerListener {
    void onSend(String source, OutgoingMessageEnvelope envelope);
  }
}
