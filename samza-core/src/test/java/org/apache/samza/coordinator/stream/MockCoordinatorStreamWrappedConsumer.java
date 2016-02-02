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

package org.apache.samza.coordinator.stream;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.messages.SetChangelogMapping;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.BlockingEnvelopeMap;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * A mock SystemConsumer that pretends to be a coordinator stream. The mock will
 * take all configs given to it, and put them into the coordinator stream's
 * SystemStreamPartition. This is useful in cases where config needs to be
 * quickly passed from a unit test into the JobCoordinator.
 */
public class MockCoordinatorStreamWrappedConsumer extends BlockingEnvelopeMap {
  private final static ObjectMapper MAPPER = SamzaObjectMapper.getObjectMapper();
  public final static String CHANGELOGPREFIX = "ch:";
  public final CountDownLatch blockConsumerPoll = new CountDownLatch(1);
  public boolean blockpollFlag = false;

  private final SystemStreamPartition systemStreamPartition;
  private final Config config;

  public MockCoordinatorStreamWrappedConsumer(SystemStreamPartition systemStreamPartition, Config config) {
    super();
    this.config = config;
    this.systemStreamPartition = systemStreamPartition;
  }

  public void start() {
    convertConfigToCoordinatorMessage(config);
  }

  public void addMoreMessages(Config config) {
    convertConfigToCoordinatorMessage(config);
  }

  public void addMessageEnvelope(IncomingMessageEnvelope envelope) throws IOException, InterruptedException {
    put(systemStreamPartition, envelope);
    setIsAtHead(systemStreamPartition, true);
  }

  private void convertConfigToCoordinatorMessage(Config config) {
    try {
      for (Map.Entry<String, String> configPair : config.entrySet()) {
        byte[] keyBytes = null;
        byte[] messgeBytes = null;
        if (configPair.getKey().startsWith(CHANGELOGPREFIX)) {
          String[] changelogInfo = configPair.getKey().split(":");
          String changeLogPartition = configPair.getValue();
          SetChangelogMapping changelogMapping = new SetChangelogMapping(changelogInfo[1], changelogInfo[2], Integer.parseInt(changeLogPartition));
          keyBytes = MAPPER.writeValueAsString(changelogMapping.getKeyArray()).getBytes("UTF-8");
          messgeBytes = MAPPER.writeValueAsString(changelogMapping.getMessageMap()).getBytes("UTF-8");
        } else {
          SetConfig setConfig = new SetConfig("source", configPair.getKey(), configPair.getValue());
          keyBytes = MAPPER.writeValueAsString(setConfig.getKeyArray()).getBytes("UTF-8");
          messgeBytes = MAPPER.writeValueAsString(setConfig.getMessageMap()).getBytes("UTF-8");
        }
        // The ssp here is the coordinator ssp (which is always fixed) and not the task ssp.
        put(systemStreamPartition, new IncomingMessageEnvelope(systemStreamPartition, "", keyBytes, messgeBytes));
      }
      setIsAtHead(systemStreamPartition, true);
    } catch (Exception e) {
      throw new SamzaException(e);
    }
  }

  @Override
  public Map<SystemStreamPartition, List<IncomingMessageEnvelope>> poll(
    Set<SystemStreamPartition> systemStreamPartitions, long timeout)
    throws InterruptedException {

    if (blockpollFlag) {
      blockConsumerPoll.await();
    }

    return super.poll(systemStreamPartitions, timeout);
  }

  public CountDownLatch blockPool() {
    blockpollFlag = true;
    return blockConsumerPoll;
  }


  public void stop() {}
}
