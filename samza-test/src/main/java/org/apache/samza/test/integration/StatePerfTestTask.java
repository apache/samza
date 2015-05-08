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

package org.apache.samza.test.integration;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskCoordinator.RequestScope;

/**
 * A simple performance test that just reads in messages and writes them to a state store as quickly as possible and periodically prints out throughput numbers
 */
public class StatePerfTestTask implements StreamTask, InitableTask {

  private static final int LOG_INTERVAL = 100000;

  private KeyValueStore<String, String> store;
  private int count = 0;
  private long start = System.currentTimeMillis();

  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) {
    this.store = (KeyValueStore<String, String>) context.getStore("mystore");
  }
  
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    store.put((String) envelope.getMessage(), (String) envelope.getMessage());
    count++;
    if (count % LOG_INTERVAL == 0) {
      double ellapsedSecs = (System.currentTimeMillis() - start) / 1000.0;
      System.out.println(String.format("Throughput = %.2f messages/sec.", count / ellapsedSecs));
      start = System.currentTimeMillis();
      count = 0;
      coordinator.commit(RequestScope.ALL_TASKS_IN_CONTAINER);
    }
  }

}
