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

package org.apache.samza.sql;

import org.apache.samza.operators.KV;
import org.apache.samza.sql.data.SamzaSqlRelMsgMetadata;


/**
 * Represents the message transformed from IncomingMessageEnvelope by SamzaSqlInputTransformer
 * it contains the event message and its metadata
 */
public class SamzaSqlInputMessage {
  private final KV<Object, Object> keyAndMessageKV;
  private final SamzaSqlRelMsgMetadata metadata;

  /**
   * Constructs a new SamzaSqMessage given the input arguments
   * @param keyAndMessageKV
   * @param metadata
   */
  private SamzaSqlInputMessage(KV<Object, Object> keyAndMessageKV, SamzaSqlRelMsgMetadata metadata) {
    this.keyAndMessageKV = keyAndMessageKV;
    this.metadata = metadata;
  }

  /**
   * Constructs a new SamzaSqMessage given the input arguments
   * @param keyAndMessageKV key-value of the message
   * @param metadata metadata of the message
   * @return new object of SamzaSqlInputMessage type
   */
  public static SamzaSqlInputMessage of(KV<Object, Object> keyAndMessageKV, SamzaSqlRelMsgMetadata metadata) {
    return new SamzaSqlInputMessage(keyAndMessageKV, metadata);
  }

  public KV<Object, Object> getKeyAndMessageKV() {
    return keyAndMessageKV;
  }

  public SamzaSqlRelMsgMetadata getMetadata() {
    return metadata;
  }
}
