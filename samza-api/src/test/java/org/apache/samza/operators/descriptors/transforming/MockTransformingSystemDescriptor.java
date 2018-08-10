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

@SuppressWarnings("unchecked")
public class MockTransformingSystemDescriptor extends TransformingSystemDescriptor<Long, MockTransformingSystemDescriptor> {
  private static final String FACTORY_CLASS_NAME = "org.apache.samza.IMETransformingSystemFactory";

  public MockTransformingSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, ime -> 1L);
  }

  @Override
  public MockTransformingInputDescriptor<Long> getInputDescriptor(String streamId, Serde serde) {
    return new MockTransformingInputDescriptor<>(streamId, this, null, serde);
  }

  @Override
  public <StreamMessageType> MockTransformingInputDescriptor<StreamMessageType> getInputDescriptor(String streamId, InputTransformer<StreamMessageType> transformer, Serde serde) {
    return new MockTransformingInputDescriptor<>(streamId, this, transformer, serde);
  }

  @Override
  public <StreamMessageType> MockTransformingOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new MockTransformingOutputDescriptor<>(streamId, this, serde);
  }
}
