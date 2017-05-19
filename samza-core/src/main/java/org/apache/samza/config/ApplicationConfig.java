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

import org.apache.samza.runtime.UUIDGenerator;


/**
 * Accessors for configs associated with Application scope
 */
public class ApplicationConfig extends MapConfig {
  /**
   * <p>processor.id is similar to the logical containerId generated in Samza. However, in addition to identifying the JVM
   * of the processor, it also contains a segment to identify the instance of the
   * {@link org.apache.samza.processor.StreamProcessor} within the JVM. More detail can be found in
   * {@link org.apache.samza.runtime.ProcessorIdGenerator}. </p>
   * <p>
   * This is an important distinction because Samza 0.13.0 in Yarn has a 1:1 mapping between the processor and the Yarn
   * container (JVM). However, Samza in an embedded execution can contain more than one processor within the same JVM.
   * </p>
   * <b>Note:</b>This identifier has to be unique across the instances of StreamProcessors.
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
  public static final String APP_NAME = "app.name";
  public static final String APP_ID = "app.id";
  public static final String APP_CLASS = "app.class";

  public ApplicationConfig(Config config) {
    super(config);
  }

  public String getAppProcessorIdGeneratorClass() {
    return get(APP_PROCESSOR_ID_GENERATOR_CLASS, UUIDGenerator.class.getName());
  }

  public String getAppName() {
    return get(APP_NAME, get(JobConfig.JOB_NAME()));
  }

  public String getAppId() {
    return get(APP_ID, get(JobConfig.JOB_ID(), "1"));
  }

  public String getAppClass() {
    return get(APP_CLASS, null);
  }

  /**
   * returns full application id
   * @return full app id
   */
  public String getGlobalAppId() {
    return String.format("app-%s-%s", getAppName(), getAppId());
  }

  @Deprecated
  public String getProcessorId() {
    return get(PROCESSOR_ID, null);
  }

}
