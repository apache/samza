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

import org.apache.samza.SamzaException;
import org.apache.samza.serializers.JsonSerdeV2;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;


/**
 * Kinesis system consumer related checkpoint information that is stored in the IncomingMessageEnvelope offset.
 *
 * It contains the following metadata:
 * <ul>
 *   <li> shardId: Kinesis stream shardId.
 *   <li> seqNumber: sequence number in the above shard.
 * </ul>
 *
 * Please note that the source of truth for checkpointing is the AWS dynamoDB table corresponding to the application.
 * The offset that is stored in Samza checkpoint topic is not used.
 */
public class KinesisSystemConsumerOffset {

  @JsonProperty("shardId")
  private String shardId;
  @JsonProperty("seqNumber")
  private String seqNumber;

  @JsonCreator
  KinesisSystemConsumerOffset(@JsonProperty("shardId") String shardId,
      @JsonProperty("seqNumber") String seqNumber) {
    this.shardId = shardId;
    this.seqNumber = seqNumber;
  }

  String getShardId() {
    return shardId;
  }

  String getSeqNumber() {
    return seqNumber;
  }

  static KinesisSystemConsumerOffset parse(String metadata) {
    JsonSerdeV2<KinesisSystemConsumerOffset> jsonSerde = new JsonSerdeV2<>(KinesisSystemConsumerOffset.class);
    byte[] bytes;
    try {
      bytes = metadata.getBytes("UTF-8");
    } catch (Exception e) {
      throw new SamzaException(e);
    }
    return jsonSerde.fromBytes(bytes);
  }

  @SuppressWarnings("unchecked")
  @Override
  public String toString() {
    JsonSerdeV2<KinesisSystemConsumerOffset> jsonSerde = new JsonSerdeV2<>(KinesisSystemConsumerOffset.class);
    return new String(jsonSerde.toBytes(this));
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof KinesisSystemConsumerOffset)) {
      return false;
    }

    String thatShardId = ((KinesisSystemConsumerOffset) o).getShardId();
    if (!(shardId == null ? thatShardId == null : shardId.equals(thatShardId))) {
      return false;
    }
    String thatSeqNumber = ((KinesisSystemConsumerOffset) o).getSeqNumber();
    if (!(seqNumber == null ? thatSeqNumber == null : seqNumber.equals(thatSeqNumber))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int result = shardId.hashCode();
    result = 31 * result + seqNumber.hashCode();
    return result;
  }
}

