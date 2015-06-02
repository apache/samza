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

import org.apache.samza.sql.api.data.Data;
import org.apache.samza.sql.api.data.EntityName;
import org.apache.samza.sql.api.data.Tuple;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.sql.LongOffset;
import org.apache.samza.system.sql.Offset;


/**
 * This class implements a {@link org.apache.samza.sql.api.data.Tuple} that encapsulates an {@link org.apache.samza.system.IncomingMessageEnvelope} from the system
 *
 */
public class IncomingMessageTuple implements Tuple {
  /**
   * Incoming message envelope
   */
  private final IncomingMessageEnvelope imsg;

  /**
   * The entity name for the incoming system stream
   */
  private final EntityName strmEntity;

  /**
   * The receive time of this incoming message
   */
  private final long recvTimeNano;

  /**
   * Ctor to create a {@code IncomingMessageTuple} from {@link org.apache.samza.system.IncomingMessageEnvelope}
   *
   * @param imsg The incoming system message
   */
  public IncomingMessageTuple(IncomingMessageEnvelope imsg) {
    this.imsg = imsg;
    this.strmEntity =
        EntityName.getStreamName(String.format("%s:%s", imsg.getSystemStreamPartition().getSystem(), imsg
            .getSystemStreamPartition().getStream()));
    this.recvTimeNano = System.nanoTime();
  }

  @Override
  public Data getMessage() {
    return (Data) this.imsg.getMessage();
  }

  @Override
  public boolean isDelete() {
    return false;
  }

  @Override
  public Data getKey() {
    return (Data) this.imsg.getKey();
  }

  @Override
  public EntityName getEntityName() {
    return this.strmEntity;
  }

  @Override
  public long getCreateTimeNano() {
    // TODO: this is wrong and just to keep as an placeholder. It should be replaced by the message publish time when the publish timestamp is available in the message metadata
    return this.recvTimeNano;
  }

  @Override
  public Offset getOffset() {
    // TODO: need to add offset factory to generate different types of offset. This is just a placeholder,
    // assuming incoming message carries long value as offset (i.e. Kafka case)
    return new LongOffset(this.imsg.getOffset());
  }
}
