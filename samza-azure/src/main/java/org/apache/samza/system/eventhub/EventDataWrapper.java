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

import com.microsoft.azure.eventhubs.EventData;

/**
 * Simpler wrapper of {@link EventData} events with the decrypted payload
 */
public class EventDataWrapper {
  private final EventData eventData;
  private final byte[] body;

  public EventDataWrapper(EventData eventData, byte[] body) {
    this.eventData = eventData;
    this.body = body;
  }

  public EventData getEventData() {
    return eventData;
  }

  /**
   * @return the body of decrypted body of the message. In case no encryption is setup for this topic,
   * just returns the body of the message.
   */
  public byte[] getDecryptedBody() {
    return body;
  }

  @Override
  public String toString() {
    return "EventDataWrapper: body: " + (new String(body)) + ",  EventData " + eventData;
  }

}
