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

package org.apache.samza.test.integration.join;

import org.apache.samza.config.Config;
import org.apache.samza.container.TaskName;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.TaskCoordinator.RequestScope;
import org.apache.samza.task.WindowableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("unchecked")
public class Emitter implements StreamTask, InitableTask, WindowableTask {
  
  private static Logger logger = LoggerFactory.getLogger(Emitter.class);
  
  private static final String EPOCH = "the-epoch";
  private static final String COUNT = "the-count";

  private KeyValueStore<String, String> state;
  private int max;
  private TaskName taskName;

  @Override
  public void init(Config config, TaskContext context) {
    this.state = (KeyValueStore<String, String>) context.getStore("emitter-state");
    this.taskName = context.getTaskName();
    this.max = config.getInt("count");
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    if (envelope.getSystemStreamPartition().getStream().equals("epoch")) {
      int newEpoch = Integer.parseInt((String) envelope.getMessage());
      logger.info("New epoch in message - " + newEpoch);

      Integer epoch = getInt(EPOCH);
      if (epoch == null || newEpoch == epoch)
        return;
      if (newEpoch < epoch)
        throw new IllegalArgumentException("Got new epoch " + newEpoch + " which is less than current epoch " + epoch);
      
      // it's a new era, reset current epoch and count
      logger.info("Epoch: " + newEpoch);
      this.state.put(EPOCH, Integer.toString(newEpoch));
      this.state.put(COUNT, "0");
      coordinator.commit(RequestScope.ALL_TASKS_IN_CONTAINER);
    }
  }
  
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    Integer epoch = getInt(EPOCH);
    if (epoch == null) {
      resetEpoch();
      return;
    }
    int counter = getInt(COUNT);
    if (counter < max) {
      logger.info("Emitting: " + counter + ", epoch = " + epoch + ", task = " + taskName);
      OutgoingMessageEnvelope envelope = new OutgoingMessageEnvelope(new SystemStream("kafka", "emitted"), Integer.toString(counter), epoch + "-" + taskName.toString());
      collector.send(envelope);
      this.state.put(COUNT, Integer.toString(getInt(COUNT) + 1));
    }
  }
  
  private void resetEpoch() {
    logger.info("Resetting epoch to 0");
    state.put(EPOCH, "0");
    state.put(COUNT, "0");
  }
  
  private Integer getInt(String key) {
    String value = this.state.get(key);
    return value == null ? null : Integer.parseInt(value);
  }

}
