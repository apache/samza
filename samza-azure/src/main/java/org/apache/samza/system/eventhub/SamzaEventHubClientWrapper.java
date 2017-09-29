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
import com.microsoft.azure.servicebus.ServiceBusException;
import org.apache.samza.SamzaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SamzaEventHubClientWrapper implements EventHubClientWrapper {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaEventHubClientWrapper.class.getName());
  private static final String EVENTHUB_REMOTE_HOST_FORMAT = "%s.servicebus.windows.net";

  private EventHubClient eventHubClient;

  private final String eventHubNamespace;
  private final String entityPath;
  private final String sasKeyName;
  private final String sasKey;

  public SamzaEventHubClientWrapper(String eventHubNamespace, String entityPath, String sasKeyName, String sasKey) {
    this.eventHubNamespace = eventHubNamespace;
    this.entityPath = entityPath;
    this.sasKeyName = sasKeyName;
    this.sasKey = sasKey;
  }

  public void init() {
    String remoteHost = String.format(EVENTHUB_REMOTE_HOST_FORMAT, eventHubNamespace);
    try {
      ConnectionStringBuilder connectionStringBuilder =
              new ConnectionStringBuilder(eventHubNamespace, entityPath, sasKeyName, sasKey);

      eventHubClient = EventHubClient.createFromConnectionStringSync(connectionStringBuilder.toString());
    } catch (IOException ioe) {
      throw new IllegalStateException(
              "Failed to connect to remote host " + remoteHost + ":" + ClientConstants.AMQPS_PORT, ioe);
    } catch (ServiceBusException e) {
      String msg = String.format("Creation of event hub client failed for eventHub %s %s %s %s with exception",
              entityPath, eventHubNamespace, sasKeyName, sasKey);
      LOG.error(msg, e);
      throw new SamzaException(msg, e);
    }
  }

  public EventHubClient getEventHubClient() {
    return eventHubClient;
  }


  public void close(long timeoutMS) {
    if (timeoutMS <= 0) {
      try {
        eventHubClient.closeSync();
      } catch (ServiceBusException e) {
        LOG.warn("Closing the event hub client failed ", e);
      }
    } else {
      CompletableFuture<Void> future = eventHubClient.close();
      try {
        future.get(timeoutMS, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | InterruptedException | TimeoutException e) {
        LOG.warn("Closing the event hub client failed ", e);
      }
    }

  }

}
