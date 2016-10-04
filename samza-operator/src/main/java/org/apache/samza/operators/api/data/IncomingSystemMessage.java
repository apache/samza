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
package org.apache.samza.operators.api.data;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemStreamPartition;


/**
 * This class implements a {@link Message} that encapsulates an {@link IncomingMessageEnvelope} from the system
 *
 */
public class IncomingSystemMessage implements Message<Object, Object>, InputSystemMessage<Offset> {
  /**
   * Incoming message envelope
   */
  private final IncomingMessageEnvelope imsg;

  /**
   * The receive time of this incoming message
   */
  private final long recvTimeNano;

  /**
   * Ctor to create a {@code IncomingSystemMessage} from {@link IncomingMessageEnvelope}
   *
   * @param imsg The incoming system message
   */
  public IncomingSystemMessage(IncomingMessageEnvelope imsg) {
    this.imsg = imsg;
    this.recvTimeNano = System.nanoTime();
  }

  @Override
  public Object getMessage() {
    return this.imsg.getMessage();
  }

  @Override
  public Object getKey() {
    return this.imsg.getKey();
  }

  @Override
  public long getTimestamp() {
    return this.recvTimeNano;
  }

  @Override
  public Offset getOffset() {
    // TODO: need to add offset factory to generate different types of offset. This is just a placeholder,
    // assuming incoming message carries long value as offset (i.e. Kafka case)
    return new LongOffset(this.imsg.getOffset());
  }

  @Override
  public SystemStreamPartition getSystemStreamPartition() {
    return imsg.getSystemStreamPartition();
  }
}
