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

import org.apache.samza.operators.descriptors.base.system.OutputDescriptorProvider;
import org.apache.samza.operators.descriptors.base.system.SimpleInputDescriptorProvider;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.serializers.Serde;

public class ExampleSimpleSystemDescriptor extends SystemDescriptor<ExampleSimpleSystemDescriptor>
    implements SimpleInputDescriptorProvider, OutputDescriptorProvider {
  private static final String FACTORY_CLASS_NAME = "org.apache.kafka.KafkaSystemFactory";

  public ExampleSimpleSystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, null, null);
  }

  @Override
  public <StreamMessageType> ExampleSimpleInputDescriptor<StreamMessageType> getInputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new ExampleSimpleInputDescriptor<>(streamId, this, null, serde);
  }

  @Override
  public <StreamMessageType> ExampleSimpleOutputDescriptor<StreamMessageType> getOutputDescriptor(String streamId, Serde<StreamMessageType> serde) {
    return new ExampleSimpleOutputDescriptor<>(streamId, this, serde);
  }
}
