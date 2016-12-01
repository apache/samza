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

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;


/**
 * A {@link Message} implementation that provides additional information about its input {@link SystemStreamPartition}
 * and its {@link Offset} within the {@link SystemStreamPartition}.
 * <p>
 * Note: the {@link Offset} is only unique and comparable within its {@link SystemStreamPartition}.
 */
public class IncomingSystemMessage implements Message<Object, Object> {

  private final IncomingMessageEnvelope ime;

  /**
   * Creates an {@code IncomingSystemMessage} from the {@link IncomingMessageEnvelope}.
   *
   * @param ime  the incoming message envelope from the input system.
   */
  public IncomingSystemMessage(IncomingMessageEnvelope ime) {
    this.ime = ime;
  }

  @Override
  public Object getKey() {
    return this.ime.getKey();
  }

  @Override
  public Object getMessage() {
    return this.ime.getMessage();
  }

  public Offset getOffset() {
    // TODO: need to add offset factory to generate different types of offset. This is just a placeholder,
    // assuming incoming message carries long value as offset (i.e. Kafka case)
    return new LongOffset(this.ime.getOffset());
  }

  public SystemStreamPartition getSystemStreamPartition() {
    return this.ime.getSystemStreamPartition();
  }
}
