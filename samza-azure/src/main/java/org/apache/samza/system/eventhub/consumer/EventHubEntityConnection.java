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

package org.apache.samza.system.eventhub.consumer;

import com.microsoft.azure.eventhubs.PartitionReceiveHandler;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;
import com.microsoft.azure.servicebus.StringUtil;
import org.apache.samza.SamzaException;
import org.apache.samza.system.eventhub.EventHubClientFactory;
import org.apache.samza.system.eventhub.EventHubClientWrapper;
import org.apache.samza.system.eventhub.EventHubConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Map;
import java.util.TreeMap;

public class EventHubEntityConnection {
  private static final Logger LOG = LoggerFactory.getLogger(EventHubEntityConnection.class);
  final Map<Integer, String> offsets = new TreeMap<>();
  final Map<Integer, PartitionReceiveHandler> handlers = new TreeMap<>();
  private final String namespace;
  private final String entityPath;
  private final String sasKeyName;
  private final String sasKey;
  private final String consumerName;
  private final EventHubConfig config;
  private final Map<Integer, PartitionReceiver> receivers = new TreeMap<>();
  private EventHubClientWrapper ehClientWrapper;
  private final EventHubClientFactory eventHubClientFactory = new EventHubClientFactory();
  private boolean isStarted = false;

  EventHubEntityConnection(String namespace, String entityPath, String sasKeyName, String sasKey,
                           String consumerName, EventHubConfig config) {
    this.namespace = namespace;
    this.entityPath = entityPath;
    this.sasKeyName = sasKeyName;
    this.sasKey = sasKey;
    this.consumerName = consumerName;
    this.config = config;
  }

  // add partitions and handlers for this connection. This can be called multiple times
  // for multiple partitions, but needs to be called before connectAndStart()
  synchronized void addPartition(int partitionId, String offset, PartitionReceiveHandler handler) {
    if (isStarted) {
      LOG.warn("Trying to add partition when the connection has already started.");
      return;
    }
    offsets.put(partitionId, offset);
    handlers.put(partitionId, handler);
  }

  // establish the connection and start consuming events
  synchronized void connectAndStart() {
    isStarted = true;
    try {
      LOG.info(String.format("Starting connection for namespace=%s, entity=%s ", namespace, entityPath));
      // upon the instantiation of the client, the connection will be established
      ehClientWrapper = eventHubClientFactory
              .getEventHubClient(namespace, entityPath, sasKeyName, sasKey, config);
      ehClientWrapper.init();
      for (Map.Entry<Integer, String> entry : offsets.entrySet()) {
        Integer id = entry.getKey();
        String offset = entry.getValue();
        try {
          PartitionReceiver receiver;
          if (StringUtil.isNullOrWhiteSpace(offset)) {
            throw new SamzaException(
                    String.format("Invalid offset %s namespace=%s, entity=%s", offset, namespace, entityPath));
          }
          if (offset.equals(EventHubSystemConsumer.END_OF_STREAM)) {
            receiver = ehClientWrapper.getEventHubClient()
                    .createReceiverSync(consumerName, id.toString(), Instant.now());
          } else {
            receiver = ehClientWrapper.getEventHubClient()
                    .createReceiverSync(consumerName, id.toString(), offset,
                            !offset.equals(EventHubSystemConsumer.START_OF_STREAM));
          }
          receiver.setReceiveHandler(handlers.get(id));
          receivers.put(id, receiver);
        } catch (Exception e) {
          throw new SamzaException(
                  String.format("Failed to create receiver for EventHubs: namespace=%s, entity=%s, partitionId=%d",
                          namespace, entityPath, id), e);
        }
      }
    } catch (Exception e) {
      throw new SamzaException(
              String.format("Failed to create connection to EventHubs: namespace=%s, entity=%s",
                      namespace, entityPath),
              e);
    }
    LOG.info(String.format("Connection successfully started for namespace=%s, entity=%s ", namespace, entityPath));
  }

  synchronized void stop() {
    LOG.info(String.format("Stopping connection for namespace=%s, entity=%s ", namespace, entityPath));
    try {
      for (PartitionReceiver receiver : receivers.values()) {
        receiver.closeSync();
      }
      ehClientWrapper.close(0);
    } catch (ServiceBusException e) {
      throw new SamzaException(
              String.format("Failed to stop connection for namespace=%s, entity=%s ", namespace, entityPath), e);
    }
    isStarted = false;
    offsets.clear();
    handlers.clear();
    LOG.info(String.format("Connection for namespace=%s, entity=%s stopped", namespace, entityPath));
  }
}
