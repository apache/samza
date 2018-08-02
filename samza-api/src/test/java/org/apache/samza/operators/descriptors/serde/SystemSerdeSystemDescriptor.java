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
package org.apache.samza.operators.descriptors.serde;

import org.apache.samza.operators.descriptors.base.system.SimpleSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;

public class SystemSerdeSystemDescriptor extends SimpleSystemDescriptor<String, SystemSerdeSystemDescriptor> {
  private static final String FACTORY_CLASS_NAME = "org.apache.kafka.KafkaSystemFactory";

  public SystemSerdeSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, new StringSerde());
  }

  public SystemSerdeInputDescriptor<String> getInputDescriptor(String streamId) {
    return new SystemSerdeInputDescriptor<>(streamId, this, null, getSerde());
  }

  public <TStream> SystemSerdeInputDescriptor<TStream> getInputDescriptor(String streamId, Serde<TStream> serde) {
    return new SystemSerdeInputDescriptor<>(streamId, this, null, serde);
  }

  @Override
  public <StreamMessageType> SystemSerdeInputDescriptor<StreamMessageType> getInputDescriptor(String streamId, InputTransformer<StreamMessageType> transformer) {
    return new SystemSerdeInputDescriptor<>(streamId, this, transformer, getSerde());
  }

  @Override
  public <StreamMessageType> SystemSerdeInputDescriptor<StreamMessageType> getInputDescriptor(String streamId, InputTransformer<StreamMessageType> transformer, Serde serde) {
    return new SystemSerdeInputDescriptor<>(streamId, this, transformer, serde);
  }

  @Override
  public SystemSerdeOutputDescriptor<String> getOutputDescriptor(String streamId) {
    return new SystemSerdeOutputDescriptor<>(streamId, this, getSerde());
  }

  @Override
  public <StreamMessageType> SystemSerdeOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new SystemSerdeOutputDescriptor<>(streamId, this, serde);
  }
}
