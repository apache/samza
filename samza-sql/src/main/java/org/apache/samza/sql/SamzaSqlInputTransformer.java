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

import java.time.Instant;
import net.jodah.failsafe.internal.util.Assert;
import org.apache.samza.operators.KV;
import org.apache.samza.sql.data.SamzaSqlRelMsgMetadata;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.descriptors.InputTransformer;


/**
 * SamzaSqlInputTransformer:
 *   Input Transformer for SamzaSQL that consumes {@link IncomingMessageEnvelope} (IME) and produces
 *   {@link SamzaSqlInputMessage} so that the event metadata (currently eventTime and arrivalTime) are copied
 *   from the IME to the SamzaSQL layer through the {@link SamzaSqlInputMessage}
 */

public class SamzaSqlInputTransformer implements InputTransformer {

  @Override
  public Object apply(IncomingMessageEnvelope ime) {
    Assert.notNull(ime, "ime is null");
    KV<Object, Object> keyAndMessageKV = KV.of(ime.getKey(), ime.getMessage());
    SamzaSqlRelMsgMetadata metadata = new SamzaSqlRelMsgMetadata(
        (ime.getEventTime() == 0) ? "" : Instant.ofEpochMilli(ime.getEventTime()).toString(),
        (ime.getArrivalTime() == 0) ? "" : Instant.ofEpochMilli(ime.getArrivalTime()).toString(), null);
    SamzaSqlInputMessage samzaMsg = SamzaSqlInputMessage.of(keyAndMessageKV, metadata);
    return  samzaMsg;
  }
}
