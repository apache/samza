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

package org.apache.samza.system.eventhub;

import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.impl.ClientConstants;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.impl.RetryExponential;
import com.microsoft.azure.eventhubs.RetryPolicy;
import com.microsoft.azure.eventhubs.EventHubException;
import java.util.concurrent.Executors;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SamzaEventHubClientManager implements EventHubClientManager {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaEventHubClientManager.class.getName());

  private static final String EVENTHUB_REMOTE_HOST_FORMAT = "%s.servicebus.windows.net";

  private static final Duration MIN_RETRY_BACKOFF = Duration.ofMillis(100);
  private static final Duration MAX_RETRY_BACKOFF = Duration.ofMillis(11000);
  private static final int MAX_RETRY_COUNT = 100;
  private static final String SAMZA_EVENTHUB_RETRY = "SAMZA_CONNECTOR_RETRY";
  private final int numClientThreads;

  private EventHubClient eventHubClient;

  private final String eventHubNamespace;
  private final String entityPath;
  private final String sasKeyName;
  private final String sasKey;
  private final RetryPolicy retryPolicy;

  public SamzaEventHubClientManager(String eventHubNamespace, String entityPath, String sasKeyName, String sasKey,
      Integer numClientThreads) {
    this(eventHubNamespace, entityPath, sasKeyName, sasKey,
            new RetryExponential(MIN_RETRY_BACKOFF, MAX_RETRY_BACKOFF, MAX_RETRY_COUNT, SAMZA_EVENTHUB_RETRY), numClientThreads);
  }

  public SamzaEventHubClientManager(String eventHubNamespace, String entityPath, String sasKeyName, String sasKey,
                                    RetryPolicy retryPolicy, int numClientThreads) {
    this.eventHubNamespace = eventHubNamespace;
    this.entityPath = entityPath;
    this.sasKeyName = sasKeyName;
    this.sasKey = sasKey;
    this.retryPolicy = retryPolicy;
    this.numClientThreads = numClientThreads;
  }

  @Override
  public void init() {
    String remoteHost = String.format(EVENTHUB_REMOTE_HOST_FORMAT, eventHubNamespace);
    try {
      ConnectionStringBuilder connectionStringBuilder =
              new ConnectionStringBuilder()
          .setNamespaceName(eventHubNamespace)
          .setEventHubName(entityPath)
          .setSasKeyName(sasKeyName)
          .setSasKey(sasKey);

      eventHubClient = EventHubClient.createSync(connectionStringBuilder.toString(), retryPolicy,
          Executors.newFixedThreadPool(numClientThreads));
    } catch (IOException | EventHubException e) {
      String msg = String.format("Creation of EventHub client failed for eventHub EntityPath: %s on remote host %s:%d",
              entityPath, remoteHost, ClientConstants.AMQPS_PORT);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }
  }

  @Override
  public EventHubClient getEventHubClient() {
    return eventHubClient;
  }

  @Override
  public void close(long timeoutMS) {
    try {
      if (timeoutMS == EventHubClientManager.BLOCK_UNTIL_CLOSE) {
        eventHubClient.closeSync();
      } else {
        CompletableFuture<Void> future = eventHubClient.close();
        future.get(timeoutMS, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      LOG.error("Closing the EventHub client failed", e);
    }
  }

}
