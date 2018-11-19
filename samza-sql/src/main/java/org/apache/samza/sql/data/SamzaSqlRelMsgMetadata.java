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

package org.apache.samza.sql.data;

import java.io.Serializable;
import java.time.Instant;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Metadata of Samza Sql Rel Message. Conains metadata about the corresponding event or
 * relational row of a table. Used as member of the {@link SamzaSqlRelMessage}.
 */
public class SamzaSqlRelMsgMetadata implements Serializable {
  @JsonProperty("eventTime")
  private final String eventTime;

  public SamzaSqlRelMsgMetadata() {
    eventTime = Instant.now().toString();
  }

  public SamzaSqlRelMsgMetadata(String eventTime) {
    this.eventTime = eventTime;
  }

  @JsonProperty("eventTime")
  public String getEventTime() { return eventTime;}

  @Override
  public String toString() {
    return "[Metadata:{" + eventTime + "}]";
  }

}
