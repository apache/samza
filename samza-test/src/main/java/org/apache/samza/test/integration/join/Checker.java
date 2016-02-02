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
import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Checker implements StreamTask, WindowableTask, InitableTask {
  
  private static Logger logger = LoggerFactory.getLogger(Checker.class);

  private static final String CURRENT_EPOCH = "current-epoch";

  private KeyValueStore<String, String> store;
  private int expectedKeys;
  private int numPartitions;
  
  @Override
  @SuppressWarnings("unchecked")
  public void init(Config config, TaskContext context) {
    this.store = (KeyValueStore<String, String>) context.getStore("checker-state");
    this.expectedKeys = config.getInt("expected.keys");
    this.numPartitions = config.getInt("num.partitions");
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String key = (String) envelope.getKey();
    String epoch = (String) envelope.getMessage();
    logger.info("Got key=" + key + ", epoch = " + epoch + " in checker...");
    checkEpoch(epoch);
    this.store.put(key, epoch);
  }
  
  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) {
    String currentEpoch = this.store.get(CURRENT_EPOCH);
    logger.info("Checking if epoch " + currentEpoch + " is complete.");
    int count = 0;
    KeyValueIterator<String, String> iter = this.store.all();

    while (iter.hasNext()) {
      Entry<String, String> entry = iter.next();
      String foundEpoch = entry.getValue();
      if (foundEpoch.equals(currentEpoch)) {
        count += 1;
      } else {
        logger.info("####### Found a different epoch! - " + foundEpoch + " Current epoch is " + currentEpoch);
      }
    }
    iter.close();
    if (count == expectedKeys + 1) {
      logger.info("Epoch " + currentEpoch + " is complete.");
      int nextEpoch = Integer.parseInt(currentEpoch) + 1;
      for (int i = 0; i < numPartitions; i++) {
        logger.info("Emitting next epoch - " + Integer.toString(i) + " -> " + Integer.toString(nextEpoch));
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "epoch"), Integer.toString(i), Integer.toString(nextEpoch)));
      }
      this.store.put(CURRENT_EPOCH, Integer.toString(nextEpoch));
    } else if (count > expectedKeys + 1) {
      throw new IllegalStateException("Got " + count + " keys, which is more than the expected " + (expectedKeys + 1));
    } else {
      logger.info("Only found " + count + " valid keys, try again later.");
    }
  }
  
  private void checkEpoch(String epoch) {
    String curr = this.store.get(CURRENT_EPOCH);
    if (curr == null)
      this.store.put(CURRENT_EPOCH, epoch);
    else {
      int currentEpochInStore = Integer.parseInt(curr);
      int currentEpochInMsg = Integer.parseInt(epoch);
      if (currentEpochInMsg <= currentEpochInStore) {
        if (currentEpochInMsg < currentEpochInStore)
          logger.info("#### Ignoring received epoch = " + epoch + " less than what is in store " + curr);
      } else { // should have curr > epoch
        throw new IllegalArgumentException("Got epoch " + epoch + " but have not yet completed " + curr);
      }
    }
  }
}
