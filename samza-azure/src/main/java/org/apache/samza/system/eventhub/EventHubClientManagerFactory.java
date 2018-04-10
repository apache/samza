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

public class EventHubClientManagerFactory {
  public EventHubClientManager getEventHubClientManager(String systemName, String streamName, EventHubConfig config) {

    String eventHubNamespace = config.getStreamNamespace(systemName, streamName);
    String entityPath = config.getStreamEntityPath(systemName, streamName);
    String sasKeyName = config.getStreamSasKeyName(systemName, streamName);
    String sasToken = config.getStreamSasToken(systemName, streamName);
    int numClientThreads = config.getNumClientThreads(systemName);

    return new SamzaEventHubClientManager(eventHubNamespace, entityPath, sasKeyName, sasToken, numClientThreads);
  }
}
