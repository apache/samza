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
import com.microsoft.azure.servicebus.ClientConstants;
import com.microsoft.azure.servicebus.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.RetryExponential;
import com.microsoft.azure.servicebus.RetryPolicy;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class SamzaEventHubClientImpl implements SamzaEventHubClient {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaEventHubClientImpl.class.getName());

  private static final String EVENTHUB_REMOTE_HOST_FORMAT = "%s.servicebus.windows.net";
  private static final Duration MIN_RETRY_BACKOFF = Duration.ofSeconds(0);
  private static final Duration MAX_RETRY_BACKOFF = Duration.ofSeconds(10);
  private static final int MAX_RETRY_COUNT = 100;
  private static final String SAMZA_EVENTHUB_RETRY = "SAMZA_CONNECTOR_RETRY";

  private EventHubClient eventHubClient;

  private final String eventHubNamespace;
  private final String entityPath;
  private final String sasKeyName;
  private final String sasKey;
  private final RetryPolicy retryPolicy;

  public SamzaEventHubClientImpl(String eventHubNamespace, String entityPath, String sasKeyName, String sasKey) {
    this(eventHubNamespace, entityPath, sasKeyName, sasKey,
            new RetryExponential(MIN_RETRY_BACKOFF, MAX_RETRY_BACKOFF, MAX_RETRY_COUNT, SAMZA_EVENTHUB_RETRY));
  }

  public SamzaEventHubClientImpl(String eventHubNamespace, String entityPath, String sasKeyName, String sasKey,
                                 RetryPolicy retryPolicy) {
    this.eventHubNamespace = eventHubNamespace;
    this.entityPath = entityPath;
    this.sasKeyName = sasKeyName;
    this.sasKey = sasKey;
    this.retryPolicy = retryPolicy;
  }

  public void init() {
    String remoteHost = String.format(EVENTHUB_REMOTE_HOST_FORMAT, eventHubNamespace);
    try {
      ConnectionStringBuilder connectionStringBuilder =
              new ConnectionStringBuilder(eventHubNamespace, entityPath, sasKeyName, sasKey);

      eventHubClient = EventHubClient.createFromConnectionStringSync(connectionStringBuilder.toString(), retryPolicy);
    } catch (IOException | ServiceBusException e) {
      String msg = String.format("Creation of EventHub client failed for eventHub %s %s %s %s on remote host %s:%d",
              entityPath, eventHubNamespace, sasKeyName, sasKey, remoteHost, ClientConstants.AMQPS_PORT);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }
  }

  public EventHubClient getEventHubClient() {
    return eventHubClient;
  }


  public void close(long timeoutMS) {
    try {
      if (timeoutMS <= 0) {
        eventHubClient.closeSync();
      } else {
        CompletableFuture<Void> future = eventHubClient.close();
        future.get(timeoutMS, TimeUnit.MILLISECONDS);
      }
    } catch (Exception e) {
      LOG.error("Closing the event hub client failed ", e);
    }
  }

}
