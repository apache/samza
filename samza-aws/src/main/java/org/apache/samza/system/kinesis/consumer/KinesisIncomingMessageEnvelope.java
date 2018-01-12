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

package org.apache.samza.system.kinesis.consumer;

import java.util.Date;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;


/**
 * Kinesis record with payload and some metadata.
 */
public class KinesisIncomingMessageEnvelope extends IncomingMessageEnvelope {
  private final String shardId;
  private final String sequenceNumber;
  private final Date approximateArrivalTimestamp;

  public KinesisIncomingMessageEnvelope(SystemStreamPartition systemStreamPartition, String offset, Object key,
      Object message, String shardId, String sequenceNumber, Date approximateArrivalTimestamp) {
    super(systemStreamPartition, offset, key, message);
    this.shardId = shardId;
    this.sequenceNumber = sequenceNumber;
    this.approximateArrivalTimestamp = approximateArrivalTimestamp;
  }

  public String getShardId() {
    return shardId;
  }

  public String getSequenceNumber() {
    return sequenceNumber;
  }

  public Date getApproximateArrivalTimestamp() {
    return approximateArrivalTimestamp;
  }

  @Override
  public String toString() {
    return "KinesisIncomingMessageEnvelope:: shardId:" + shardId + ", sequenceNumber:" + sequenceNumber
        + ", approximateArrivalTimestamp:" + approximateArrivalTimestamp + ", message:" + getMessage();
  }

}
