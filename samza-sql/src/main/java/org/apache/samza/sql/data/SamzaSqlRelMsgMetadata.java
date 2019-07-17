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
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Metadata of Samza Sql Rel Message. Contains metadata about the corresponding event or
 * relational row of a table. Used as member of the {@link SamzaSqlRelMessage}.
 */
public class SamzaSqlRelMsgMetadata implements Serializable {
  /**
   * boolean to indicate whether this message comes from a new input message or not, in case of
   * Project:flatten() is used, to be able to determine the number of original input messages
   * default is true for the case when no flatten() is used
   */
  public boolean isNewInputMessage = true;

  /**
   * Indicates whether the SamzaSqlMessage is a system message or not.
   */
  @JsonIgnore
  private boolean isSystemMessage = false;

  /**
   * Time at which the join operation started for the message.
   * If there is no join node in the operator graph, this will be -1.
   */
  public long joinStartTimeMs = -1;


  /**
   * The timestamp of when the events actually happened
   * set by and copied from the event source
   * TODO: copy eventTime through from source to RelMessage
   */
  @JsonProperty("eventTime")
  private String eventTime;

  /**
   * the timestamp of when Samza App received the event
   * TODO: set arrivalTime during conversion from IME to SamzaMessage
   */
  @JsonProperty("arrivalTime")
  private String arrivalTime;

  /**
   * the timestamp when SamzaSQL query starts processing the event
   * set by the SamzaSQL Scan operator
   */
  @JsonProperty("scanTime")
  private String scanTime;

  public SamzaSqlRelMsgMetadata(@JsonProperty("eventTime") String eventTime, @JsonProperty("arrivalTime") String arrivalTime,
      @JsonProperty("scanTime") String scanTime) {
    this.eventTime = eventTime;
    this.arrivalTime = arrivalTime;
    this.scanTime = scanTime;
  }

  public SamzaSqlRelMsgMetadata(String eventTime, String arrivalTime, String scanTime, boolean isNewInputMessage) {
    this(eventTime, arrivalTime, scanTime);
    this.isNewInputMessage = isNewInputMessage;
  }

  @JsonProperty("eventTime")
  public String getEventTime() { return eventTime;}

  public void setEventTime(String eventTime) {
    this.eventTime = eventTime;
  }

  public boolean hasEventTime() { return eventTime != null && !eventTime.isEmpty(); }

  @JsonProperty("arrivalTime")
  public String getarrivalTime() { return arrivalTime;}

  public void setArrivalTime(String arrivalTime) {
    this.arrivalTime = arrivalTime;
  }

  public boolean hasArrivalTime() { return arrivalTime != null && !arrivalTime.isEmpty(); }

  @JsonProperty("scanTime")
  public String getscanTime() { return scanTime;}

  public void setScanTime(String scanTime) {
    this.scanTime = scanTime;
  }

  public boolean hasScanTime() { return scanTime != null && !scanTime.isEmpty(); }

  @JsonIgnore
  public  void setIsSystemMessage(boolean isSystemMessage) {
    this.isSystemMessage = isSystemMessage;
  }

  @JsonIgnore
  public boolean isSystemMessage() {
    return isSystemMessage;
  }

  @Override
  public String toString() {
    return "[Metadata:{" + eventTime + " " + arrivalTime + " " + scanTime + "}]";
  }

}
