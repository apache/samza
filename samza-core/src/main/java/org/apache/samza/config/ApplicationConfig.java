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
package org.apache.samza.config;

/**
 * Accessors for configs associated with Application scope
 */
public class ApplicationConfig extends MapConfig {
  /**
   * processor.id is equivalent to containerId in samza. It is a logical identifier used by Samza for a processor.
   * In a distributed environment, this logical identifier is mapped to a physical identifier of the resource. For
   * example, Yarn provides a "Yarn containerId" for every resource it allocates.
   * In an embedded environment, this identifier is provided by the user by directly using the StreamProcessor API.
   * <p>
   * <b>Note:</b>This identifier has to be unique across the instances of StreamProcessors.
   * </p>
   * TODO: Deprecated in 0.13. After 0.13+, this id is generated using {@link org.apache.samza.runtime.ProcessorIdGenerator}
   */
  @Deprecated
  public static final String PROCESSOR_ID = "processor.id";

  /**
   * Class implementing the {@link org.apache.samza.runtime.ProcessorIdGenerator} interface
   * Used to generate a unique identifier for a {@link org.apache.samza.processor.StreamProcessor} based on the runtime
   * environment. Hence, durability of the identifier is same as the guarantees provided by the runtime environment
   */
  public static final String APP_PROCESSOR_ID_GENERATOR_CLASS = "app.processor-id-generator.class";

  public ApplicationConfig(Config config) {
    super(config);
  }

  public String getAppProcessorIdGeneratorClass() {
    return get(APP_PROCESSOR_ID_GENERATOR_CLASS, null);
  }

  @Deprecated
  public String getProcessorId() {
    return get(PROCESSOR_ID, null);
  }

}
