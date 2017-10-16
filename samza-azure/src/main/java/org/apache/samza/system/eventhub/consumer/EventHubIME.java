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

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventData.SystemProperties;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;

import java.util.Map;

/**
 * Extension of {@link IncomingMessageEnvelope} which contains {@link EventData} system and user properties metadata
 */
public class EventHubIME extends IncomingMessageEnvelope {
  private SystemProperties systemProperties;
  private Map<String, Object> userProperties;


  public EventHubIME(SystemStreamPartition systemStreamPartition, String offset, Object key, Object message, EventData eventData) {
    super(systemStreamPartition, offset, key, message);

    this.systemProperties = eventData.getSystemProperties();
    this.userProperties = eventData.getProperties();
  }

  public SystemProperties getSystemProperties() {
    return systemProperties;
  }

  public Map<String, Object> getUserProperties() {
    return userProperties;
  }
}
