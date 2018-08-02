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
package org.apache.samza.operators.descriptors.expanding;

import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.descriptors.base.system.ExpandingSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.IncomingMessageEnvelope;

public class GraphExpandingSystemDescriptor extends ExpandingSystemDescriptor<String, Long, GraphExpandingSystemDescriptor> {
  private static final String FACTORY_CLASS_NAME = "org.apache.samza.GraphExpandingSystemFactory";

  public GraphExpandingSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME,
        new StringSerde(),
        (InputTransformer<String>) IncomingMessageEnvelope::toString,
        (streamGraph, inputDescriptor) -> (MessageStream<Long>) streamGraph.getInputStream(inputDescriptor)
    );
  }

  @Override
  public GraphExpandingInputDescriptor<Long> getInputDescriptor(String streamId) {
    return new GraphExpandingInputDescriptor<>(streamId, this, null, getSerde());
  }

  @Override
  public GraphExpandingInputDescriptor<Long> getInputDescriptor(String streamId, Serde serde) {
    return new GraphExpandingInputDescriptor<>(streamId, this, null, serde);
  }

  @Override
  public GraphExpandingInputDescriptor<Long> getInputDescriptor(String streamId, InputTransformer transformer) {
    return new GraphExpandingInputDescriptor<>(streamId, this, transformer, getSerde());
  }

  @Override
  public GraphExpandingInputDescriptor<Long> getInputDescriptor(String streamId, InputTransformer transformer, Serde serde) {
    return new GraphExpandingInputDescriptor<>(streamId, this, transformer, serde);
  }

  @Override
  public GraphExpandingOutputDescriptor<String> getOutputDescriptor(String streamId) {
    return new GraphExpandingOutputDescriptor<>(streamId, this, getSerde());
  }

  @Override
  public <StreamMessageType> GraphExpandingOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new GraphExpandingOutputDescriptor<>(streamId, this, serde);
  }
}
