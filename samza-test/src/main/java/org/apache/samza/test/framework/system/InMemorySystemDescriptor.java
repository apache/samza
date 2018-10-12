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

package org.apache.samza.test.framework.system;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.InMemorySystemConfig;
import org.apache.samza.operators.descriptors.base.system.OutputDescriptorProvider;
import org.apache.samza.operators.descriptors.base.system.SimpleInputDescriptorProvider;
import org.apache.samza.operators.descriptors.base.system.SystemDescriptor;
import org.apache.samza.serializers.Serde;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.inmemory.InMemorySystemFactory;
import org.apache.samza.config.JavaSystemConfig;

/**
 * A descriptor for InMemorySystem.
 * System properties configured using a descriptor override corresponding properties provided in configuration.
 * <p>
 * Following system level configs are set by default
 * <ol>
 *   <li>"systems.%s.default.stream.samza.offset.default" = "oldest"</li>
 *   <li>"systems.%s.samza.factory" = {@link InMemorySystemFactory}</li>
 *   <li>"inmemory.scope = "Scope id generated to isolate the system in memory</li>
 * </ol>
 */
public class InMemorySystemDescriptor extends SystemDescriptor<InMemorySystemDescriptor>
    implements SimpleInputDescriptorProvider, OutputDescriptorProvider {
  private static final String FACTORY_CLASS_NAME = InMemorySystemFactory.class.getName();
  /**
   * <p>
   * The "systems.*" configs are required since the planner uses the system to get metadata about streams during
   * planning. The "jobs.job-name.systems.*" configs are required since configs generated from user provided
   * system/stream descriptors override configs originally supplied to the planner. Configs in the "jobs.job-name.*"
   * scope have the highest precedence.
   *
   * For this case, it generates following overridden configs
   * <ol>
   *      <li>"jobs.<job-name>.systems.%s.default.stream.samza.offset.default" = "oldest"</li>
   *      <li>"jobs.<job-name>.systems.%s.samza.factory" = {@link InMemorySystemFactory}</li>
   * </ol>
   *
   **/
  private String inMemoryScope;

  /**
   * Constructs a new InMemorySystemDescriptor from specified components.
   * <p>
   * Every {@link InMemorySystemDescriptor} is configured to consume from the oldest offset, since stream is in memory and
   * is used for testing purpose. System uses {@link InMemorySystemFactory} to initialize in memory streams.
   * <p>
   * @param systemName unique name of the system
   */
  public InMemorySystemDescriptor(String systemName) {
    super(systemName, FACTORY_CLASS_NAME, null, null);
    this.withDefaultStreamOffsetDefault(SystemStreamMetadata.OffsetType.OLDEST);
  }

  @Override
  public <StreamMessageType> InMemoryInputDescriptor<StreamMessageType> getInputDescriptor(
      String streamId, Serde<StreamMessageType> serde) {
    return new InMemoryInputDescriptor<StreamMessageType>(streamId, this);
  }

  @Override
  public <StreamMessageType> InMemoryOutputDescriptor<StreamMessageType> getOutputDescriptor(
      String streamId, Serde<StreamMessageType> serde) {
    return new InMemoryOutputDescriptor<StreamMessageType>(streamId, this);
  }

  /**
   * {@code inMemoryScope} defines the unique instance of InMemorySystem, that this system uses
   * This method is framework use only, users are not supposed to use it
   *
   * @param inMemoryScope acts as a unique global identifier for this instance of InMemorySystem
   * @return this system descriptor
   */
  public InMemorySystemDescriptor withInMemoryScope(String inMemoryScope) {
    this.inMemoryScope = inMemoryScope;
    return this;
  }

  @Override
  public Map<String, String> toConfig() {
    HashMap<String, String> configs = new HashMap<>(super.toConfig());
    configs.put(InMemorySystemConfig.INMEMORY_SCOPE, this.inMemoryScope);
    configs.put(String.format(JavaSystemConfig.SYSTEM_FACTORY_FORMAT, getSystemName()), FACTORY_CLASS_NAME);
    return configs;
  }

}

