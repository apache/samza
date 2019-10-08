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

package org.apache.samza.test.util;

import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;


/**
 * A Kafka event to use in testing only.
 */
public class TestKafkaEvent implements Serializable {
  // Actual content of the event.
  private String eventData;

  // Contains Integer value, which is greater than previous message id.
  private String eventId;

  public TestKafkaEvent(String eventId, String eventData) {
    this.eventData = eventData;
    this.eventId = eventId;
  }

  public String getEventId() {
    return eventId;
  }

  public String getEventData() {
    return eventData;
  }

  @Override
  public String toString() {
    return eventId + "|" + eventData;
  }

  public static TestKafkaEvent fromString(String message) {
    String[] messageComponents = StringUtils.split(message, "|");
    return new TestKafkaEvent(messageComponents[0], messageComponents[1]);
  }
}
