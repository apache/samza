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
package org.apache.samza.operators.descriptors.transforming;

import org.apache.samza.operators.descriptors.base.system.TransformingSystemDescriptor;
import org.apache.samza.operators.functions.InputTransformer;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.StringSerde;

public class IMETransformingSystemDescriptor extends TransformingSystemDescriptor<String, Long, IMETransformingSystemDescriptor> {
  private static final String FACTORY_CLASS_NAME = "org.apache.samza.IMETransformingSystemFactory";

  public IMETransformingSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, new StringSerde(), ime -> 1L);
  }

  @Override
  public IMETransformingInputDescriptor<Long> getInputDescriptor(String streamId) {
    return new IMETransformingInputDescriptor<>(streamId, this, null, getSerde());
  }

  @Override
  public IMETransformingInputDescriptor<Long> getInputDescriptor(String streamId, Serde serde) {
    return new IMETransformingInputDescriptor<>(streamId, this, null, serde);
  }

  @Override
  public <StreamMessageType> IMETransformingInputDescriptor<StreamMessageType> getInputDescriptor(String streamId, InputTransformer<StreamMessageType> transformer) {
    return new IMETransformingInputDescriptor<>(streamId, this, transformer, getSerde());
  }

  @Override
  public <StreamMessageType> IMETransformingInputDescriptor<StreamMessageType> getInputDescriptor(String streamId, InputTransformer<StreamMessageType> transformer, Serde serde) {
    return new IMETransformingInputDescriptor<>(streamId, this, transformer, serde);
  }

  @Override
  public IMETransformingOutputDescriptor<String> getOutputDescriptor(String streamId) {
    return new IMETransformingOutputDescriptor<>(streamId, this, getSerde());
  }

  @Override
  public <StreamMessageType> IMETransformingOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new IMETransformingOutputDescriptor<>(streamId, this, serde);
  }
}
