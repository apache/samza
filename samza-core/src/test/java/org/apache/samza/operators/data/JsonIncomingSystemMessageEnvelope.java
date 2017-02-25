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
package org.apache.samza.operators.data;

import org.apache.samza.system.SystemStreamPartition;


/**
 * Example input {@link MessageEnvelope} w/ Json message and string as the key.
 */

public class JsonIncomingSystemMessageEnvelope<T> implements MessageEnvelope<String, T> {

  private final String key;
  private final T data;
  private final Offset offset;
  private final SystemStreamPartition partition;

  public JsonIncomingSystemMessageEnvelope(String key, T data, Offset offset, SystemStreamPartition partition) {
    this.key = key;
    this.data = data;
    this.offset = offset;
    this.partition = partition;
  }

  @Override
  public T getMessage() {
    return this.data;
  }

  @Override
  public String getKey() {
    return this.key;
  }

  public Offset getOffset() {
    return this.offset;
  }

  public SystemStreamPartition getSystemStreamPartition() {
    return this.partition;
  }
}

