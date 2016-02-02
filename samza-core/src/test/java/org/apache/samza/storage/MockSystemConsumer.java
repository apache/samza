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

package org.apache.samza.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamPartition;

public class MockSystemConsumer implements SystemConsumer {
  public static Map<SystemStreamPartition, List<IncomingMessageEnvelope>> messages = new HashMap<SystemStreamPartition, List<IncomingMessageEnvelope>>();
  private boolean flag = true; // flag to make sure the messages only are
                               // returned once

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public void register(SystemStreamPartition systemStreamPartition, String offset) {}

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(Set<SystemStreamPartition> systemStreamPartitions, long timeout) throws InterruptedException {
    if (flag) {
      ArrayList<IncomingMessageEnvelope> list = new ArrayList<IncomingMessageEnvelope>();
      list.add(TestStorageRecovery.msg);
      messages.put(TestStorageRecovery.ssp, list);
      flag = false;
      return messages; 
    } else {
      messages.clear();
      return messages;
    }
  }
}