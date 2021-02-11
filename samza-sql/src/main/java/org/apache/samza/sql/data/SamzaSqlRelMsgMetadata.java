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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;


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
  private long eventTime = 0L;

  /**
   * the timestamp of when Samza App received the event
   * TODO: set arrivalTime during conversion from IME to SamzaMessage
   */
  @JsonProperty("arrivalTime")
  private long arrivalTime = 0L;

  /**
   * the System.nanoTime when SamzaSQL query starts processing the event
   * set by the SamzaSQL Scan operator, used by QueryTranslator to calculate
   * the Query Latency
   *
   * Note: using JsonPeoperty("scanTim") for backward compatibility
   */
  @JsonProperty("scanTime")
  private long scanTimeNanos = 0L;

  /**
   * the tiemstamp when SamzSQL wuery starts processing the event
   * set by the SamzaSQL Scan oeprator, used by QueryTranslator to calculate
   * the Queuing Latency
   */
  @JsonProperty("scanTimeMillis")
  private long scanTimeMillis = 0L;

  public SamzaSqlRelMsgMetadata(@JsonProperty("eventTime") long eventTime, @JsonProperty("arrivalTime") long arrivalTime,
      @JsonProperty("scanTime") long scanTimeNanos, @JsonProperty("scanTimeMillis") long scanTimeMillis) {
    this.eventTime = eventTime;
    this.arrivalTime = arrivalTime;
    this.scanTimeNanos = scanTimeNanos;
    this.scanTimeMillis = scanTimeMillis;
  }

  public SamzaSqlRelMsgMetadata(long eventTime, long arrivalTime, long scanTimeNanos, long scanTimeMillis,
      boolean isNewInputMessage) {
    this(eventTime, arrivalTime, scanTimeNanos, scanTimeMillis);
    this.isNewInputMessage = isNewInputMessage;
  }

  public SamzaSqlRelMsgMetadata(long eventTime, long arrivalTime) {
    this(eventTime, arrivalTime, 0L, 0L);
  }


  @JsonProperty("eventTime")
  public long getEventTime() {
    return eventTime;
  }

  public void setEventTime(long eventTime) {
    this.eventTime = eventTime;
  }

  public boolean hasEventTime() {
    return eventTime != 0L;
  }

  @JsonProperty("arrivalTime")
  public long getArrivalTime() {
    return arrivalTime;
  }

  public void setArrivalTime(long arrivalTime) {
    this.arrivalTime = arrivalTime;
  }

  public boolean hasArrivalTime() {
    return arrivalTime != 0L;
  }

  @JsonProperty("scanTime")
  public long getScanTimeNanos() {
    return scanTimeNanos;
  }

  @JsonProperty("scanTimeMillis")
  public long getScanTimeMillis() {
    return scanTimeMillis;
  }

  public void setScanTime(long scanTimeNano, long scanTimeMillis) {
    this.scanTimeNanos = scanTimeNano;
    this.scanTimeMillis = scanTimeMillis;
  }

  public boolean hasScanTime() {
    return scanTimeNanos != 0L;
  }

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
    return "[Metadata:{" + eventTime + " " + arrivalTime + " " + scanTimeNanos + " " + scanTimeMillis + "}]";
  }

}
