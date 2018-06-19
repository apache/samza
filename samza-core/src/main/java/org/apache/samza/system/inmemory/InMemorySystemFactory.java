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

package org.apache.samza.system.inmemory;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.samza.config.Config;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;


/**
 * Initial draft of in-memory {@link SystemFactory}. It is test only and not meant for production use right now.
 *
 * <p>Runtime configuration of InMemorySystem depends on a config "test.id"</p>
 * <p>
 * If {@code test.id} param is configured in the configs, it creates an isolated InMemorySystem for that test to run
 * , all the in memory streams (input/output/intermediate) are created using this isolated InMemorySystem.
 * </p>
 * <p>
 * If {@code test.id} param is not configured for an application, it shares a default InMemorySystem for all the streams
 * This system is shared between all the applications missing {@code test.id} in their configs running in the same JVM
 * using InMemorySystem
 * </p>
 */
public class InMemorySystemFactory implements SystemFactory {
  private static final String TEST_ID = "test.id";
  private static final ConcurrentHashMap<Integer, InMemoryManager> IN_MEMORY_MANAGERS = new ConcurrentHashMap<>();
  private static final InMemoryManager DEFAULT_MANAGER = new InMemoryManager();

  @Override
  public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
    return new InMemorySystemConsumer(getOrDefaultInMemoryManagerByTestId(config));
  }

  @Override
  public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
    return new InMemorySystemProducer(systemName, getOrDefaultInMemoryManagerByTestId(config));
  }

  @Override
  public SystemAdmin getAdmin(String systemName, Config config) {
    return new InMemorySystemAdmin(getOrDefaultInMemoryManagerByTestId(config));
  }

  private InMemoryManager getOrDefaultInMemoryManagerByTestId(Config config) {
    if (!config.containsKey(TEST_ID) || config.get(TEST_ID) == null) {
      return DEFAULT_MANAGER;
    }
    IN_MEMORY_MANAGERS.putIfAbsent(Integer.parseInt(config.get(TEST_ID)), new InMemoryManager());
    return IN_MEMORY_MANAGERS.get(Integer.parseInt(config.get(TEST_ID)));
  }
}
