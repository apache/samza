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

import com.microsoft.azure.eventhubs.impl.EventDataImpl;
import java.nio.charset.Charset;
import java.util.*;

public class MockEventData implements EventData {
  EventData eventData;

  private EventData.SystemProperties overridedSystemProperties;

  private MockEventData(byte[] data, String partitionKey, String offset) {
    eventData = new EventDataImpl(data);
    HashMap<String, Object> properties = new HashMap<>();
    properties.put("x-opt-offset", offset);
    properties.put("x-opt-partition-key", partitionKey);
    properties.put("x-opt-enqueued-time", new Date(System.currentTimeMillis()));
    overridedSystemProperties = new SystemProperties(properties);
  }

  public static List<EventData> generateEventData(int numEvents) {
    Random rand = new Random(System.currentTimeMillis());
    List<EventData> result = new ArrayList<>();
    for (int i = 0; i < numEvents; i++) {
      String key = "key_" + rand.nextInt();
      String message = "message:" + rand.nextInt();
      String offset = "offset_" + i;
      EventData eventData = new MockEventData(message.getBytes(Charset.defaultCharset()), key, offset);
      result.add(eventData);
    }
    return result;
  }

  @Override
  public Object getObject() {
    return eventData.getObject();
  }

  @Override
  public byte[] getBytes() {
    return eventData.getBytes();
  }

  @Override
  public Map<String, Object> getProperties() {
    return eventData.getProperties();
  }

  @Override
  public EventData.SystemProperties getSystemProperties() {
    return overridedSystemProperties;
  }

  @Override
  public void setSystemProperties(SystemProperties props) {
    this.overridedSystemProperties = props;
  }

  @Override
  public int compareTo(EventData that) {
    return Long.compare(
        this.getSystemProperties().getSequenceNumber(),
        that.getSystemProperties().getSequenceNumber()
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MockEventData)) {
      return false;
    }
    MockEventData that = (MockEventData) o;
    return Objects.equals(eventData, that.eventData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(eventData);
  }
}
