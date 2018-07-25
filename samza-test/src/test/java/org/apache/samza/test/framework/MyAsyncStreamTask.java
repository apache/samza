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

package org.apache.samza.test.framework;

import java.util.Random;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.AsyncStreamTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskCallback;
import org.apache.samza.task.TaskCoordinator;


public class MyAsyncStreamTask implements AsyncStreamTask {
  @Override
  public void processAsync(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator,
      final TaskCallback callback) {
    // Mimic a random callback delay ans send message
    RestCall call = new RestCall(envelope, collector, callback);
    call.start();
  }
}

class RestCall extends Thread {
  static Random random = new Random();
  IncomingMessageEnvelope envelope;
  MessageCollector messageCollector;
  TaskCallback callback;

  RestCall(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCallback callback) {
    this.envelope = envelope;
    this.callback = callback;
    this.messageCollector = collector;
  }

  @Override
  public void run() {
    try {
      // Let the thread sleep for a while.
      Thread.sleep(random.nextInt(150));
    } catch (InterruptedException e) {
      System.out.println("Thread " + this.getName() + " interrupted.");
    }
    Integer obj = (Integer) envelope.getMessage();
    messageCollector.send(new OutgoingMessageEnvelope(new SystemStream("async-test", "ints-out"), obj * 10));
    callback.complete();
  }
}
