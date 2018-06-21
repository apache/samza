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
 * A convenience class for fetching configs related to the {@link org.apache.samza.system.inmemory.InMemorySystemFactory}
 */
public class InMemorySystemConfig extends MapConfig {
  /**
   * <p>This Config determines Runtime behaviour of {@link org.apache.samza.system.inmemory.InMemorySystemFactory} </p>
   * <p>
   * If {@code INMEMORY_SCOPE} key is configured with a non null value in the configs, it creates an isolated
   * InMemorySystem identified by the value of {@code INMEMORY_SCOPE} in {@link org.apache.samza.system.inmemory.InMemorySystemFactory}
   * for the app while runtime. All the in memory streams (input/output/intermediate) are created using this isolated
   * InMemorySystem.
   * </p>
   * <p>
   * If {@code INMEMORY_SCOPE} key is not configured or is null for an app, it shares a default InMemorySystem
   * identified by {@code DEFAULT_INMEMORY_SCOPE} value of {@code INMEMORY_SCOPE}
   * This system is shared between all the applications missing {@code INMEMORY_SCOPE} key in their configs running
   * in the same JVM using {@link org.apache.samza.system.inmemory.InMemorySystemFactory}
   * </p>
   */
  public static final String INMEMORY_SCOPE = "inmemory.scope";

  public static final String DEFAULT_INMEMORY_SCOPE = "SAME_DEFAULT_SCOPE";

  public InMemorySystemConfig(Config config) {
    super(config);
  }
}
