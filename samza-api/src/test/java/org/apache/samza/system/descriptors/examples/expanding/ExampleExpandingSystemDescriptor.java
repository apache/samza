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
package org.apache.samza.system.descriptors.examples.expanding;

import org.apache.samza.system.descriptors.ExpandingInputDescriptorProvider;
import org.apache.samza.system.descriptors.OutputDescriptorProvider;
import org.apache.samza.system.descriptors.SystemDescriptor;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.system.descriptors.InputTransformer;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.IncomingMessageEnvelope;

public class ExampleExpandingSystemDescriptor extends SystemDescriptor<ExampleExpandingSystemDescriptor>
    implements ExpandingInputDescriptorProvider<Long>, OutputDescriptorProvider {
  private static final String FACTORY_CLASS_NAME = "org.apache.samza.GraphExpandingSystemFactory";

  public ExampleExpandingSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME,
        (InputTransformer<String>) IncomingMessageEnvelope::toString,
        (streamGraph, inputDescriptor) -> (MessageStream<Long>) streamGraph.getInputStream(inputDescriptor)
    );
  }

  @Override
  public ExampleExpandingInputDescriptor<Long> getInputDescriptor(String streamId, Serde serde) {
    return new ExampleExpandingInputDescriptor<>(streamId, this, null, serde);
  }

  @Override
  public <StreamMessageType> ExampleExpandingOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new ExampleExpandingOutputDescriptor<>(streamId, this, serde);
  }
}
