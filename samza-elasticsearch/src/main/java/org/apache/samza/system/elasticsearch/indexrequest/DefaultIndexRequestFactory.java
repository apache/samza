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

package org.apache.samza.system.elasticsearch.indexrequest;

import org.apache.samza.SamzaException;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Map;

/**
 * The default {@link IndexRequestFactory}.
 *
 * <p>Samza concepts are mapped to Elastic search concepts as follows:</p>
 *
 * <ul>
 *   <li>
 *     The index and type are derived from the stream name using
 *     the following the pattern "{index-name}/{type-name}".
 *   </li>
 *   <li>
 *     The id of the document is the {@link String} representation of the
 *     {@link OutgoingMessageEnvelope#getKey()} from {@link Object#toString()} if provided.</li>
 *   <li>
 *     The source of the document is set from the {@link OutgoingMessageEnvelope#getMessage()}.
 *     Supported types are {@link byte[]} and {@link Map} which are both
 *     passed on without serialising.
 *   </li>
 *   <li>
 *     The routing key is set from {@link String} representation of the
 *     {@link OutgoingMessageEnvelope#getPartitionKey()} from {@link Object#toString()} if provided.
 *   </li>
 * </ul>
 */
public class DefaultIndexRequestFactory implements IndexRequestFactory {

  @Override
  public IndexRequest getIndexRequest(OutgoingMessageEnvelope envelope) {
    String[] parts = envelope.getSystemStream().getStream().split("/");
    if (parts.length != 2) {
      throw new SamzaException("Elasticsearch stream name must match pattern {index}/{type}");
    }
    String index = parts[0];
    String type = parts[1];
    IndexRequest indexRequest = new IndexRequest(index, type);

    Object id = envelope.getKey();
    if (id != null) {
      indexRequest.id(id.toString());
    }

    Object partitionKey = envelope.getPartitionKey();
    if (partitionKey != null) {
      indexRequest.routing(partitionKey.toString());
    }

    Object message = envelope.getMessage();
    if (message instanceof byte[]) {
      indexRequest.source((byte[]) message);
    } else if (message instanceof Map) {
      indexRequest.source((Map) message);
    } else {
      throw new SamzaException("Unsupported message type: " + message.getClass().getCanonicalName());
    }

    return indexRequest;
  }
}
