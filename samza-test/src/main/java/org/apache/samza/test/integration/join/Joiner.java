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

import java.util.HashSet;
import java.util.Set;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class Joiner implements StreamTask, InitableTask {
  
  private static Logger logger = LoggerFactory.getLogger(Joiner.class);
  
  private KeyValueStore<String, String> store;
  private int expected;
  private TaskName taskName;

  @Override
  public void init(Config config, TaskContext context) {
    this.store = (KeyValueStore<String, String>) context.getStore("joiner-state");
    this.expected = config.getInt("num.partitions");
    this.taskName = context.getTaskName();
  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    String key = (String) envelope.getKey();
    String value = (String) envelope.getMessage();
    String[] pieces = value.split("-");
    int epoch = Integer.parseInt(pieces[0]);

    int partition = Integer.parseInt(pieces[1].split(" ")[1]);
    Partitions partitions = loadPartitions(epoch, key);
    logger.info("Joiner got epoch = " + epoch + ", partition = " + partition + ", parts = " + partitions);
    if (partitions.epoch < epoch) {
      // we are in a new era
      if (partitions.partitions.size() != expected)
        throw new IllegalArgumentException("Should have " + expected + " partitions when new epoch starts.");
      logger.info("Reseting epoch to " + epoch);
      this.store.delete(key);
      partitions.epoch = epoch;
      partitions.partitions.clear();
      partitions.partitions.add(partition);
    } else if (partitions.epoch > epoch) {
      logger.info("Ignoring message for epoch " + epoch);
    } else {
      partitions.partitions.add(partition);
      if (partitions.partitions.size() == expected) {
        logger.info("Completed: " + key + " -> " + Integer.toString(epoch));
        collector.send(new OutgoingMessageEnvelope(new SystemStream("kafka", "completed-keys"), key, Integer.toString(epoch)));
      }
    }
    this.store.put(key, partitions.toString());
    logger.info("Join store in Task " + this.taskName + " " + key + " -> " + partitions.toString());
  }
  
  private Partitions loadPartitions(int epoch, String key) {
    String current = this.store.get(key);
    Partitions partitions;
    if (current == null)
      partitions = new Partitions(epoch, new HashSet<Integer>());
    else
      partitions = Partitions.parse(current);
    return partitions;
  }
  
  private static class Partitions {
    int epoch;
    Set<Integer> partitions;
    
    public Partitions(int epoch, Set<Integer> partitions) {
      this.epoch = epoch;
      this.partitions = partitions;
    }
    
    public static Partitions parse(String s) {
      String[] pieces = s.split("\\|", -1);
      int epoch = Integer.parseInt(pieces[1]);
      Set<Integer> set = new HashSet<Integer>(pieces.length);
      for (int i = 2; i < pieces.length - 1; i++)
        set.add(Integer.parseInt(pieces[i]));
      return new Partitions(epoch, set);
    }
    
    public String toString() {
      StringBuilder b = new StringBuilder("|");
      b.append(epoch);
      b.append("|");
      for (int p: partitions) {
        b.append(p);
        b.append("|");
      }
      return b.toString();
    }
  }

}
