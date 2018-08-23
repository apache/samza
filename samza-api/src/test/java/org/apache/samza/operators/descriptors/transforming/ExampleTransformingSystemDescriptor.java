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

import org.apache.samza.operators.descriptors.base.system.OutputDescriptorProvider;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.operators.descriptors.base.system.TransformingInputDescriptorProvider;
import org.apache.samza.serializers.Serde;

public class ExampleTransformingSystemDescriptor extends SystemDescriptor<ExampleTransformingSystemDescriptor>
    implements TransformingInputDescriptorProvider<Long>, OutputDescriptorProvider {
  private static final String FACTORY_CLASS_NAME = "org.apache.samza.IMETransformingSystemFactory";

  public ExampleTransformingSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, ime -> 1L, null);
  }

  @Override
  public ExampleTransformingInputDescriptor<Long> getInputDescriptor(String streamId, Serde serde) {
    return new ExampleTransformingInputDescriptor<>(streamId, this, null, serde);
  }

  @Override
  public <StreamMessageType> ExampleTransformingOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new ExampleTransformingOutputDescriptor<>(streamId, this, serde);
  }
}
