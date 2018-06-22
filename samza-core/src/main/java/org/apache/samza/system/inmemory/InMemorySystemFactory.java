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
import org.apache.samza.config.InMemorySystemConfig;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;


/**
 * Initial draft of in-memory {@link SystemFactory}. It is test only and not meant for production use right now.
 */
public class InMemorySystemFactory implements SystemFactory {
  private static final ConcurrentHashMap<String, InMemoryManager> IN_MEMORY_MANAGERS = new ConcurrentHashMap<>();

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
    InMemorySystemConfig inMemorySystemConfig = new InMemorySystemConfig(config);
    return IN_MEMORY_MANAGERS.computeIfAbsent(inMemorySystemConfig.getInMemoryScope(), key -> new InMemoryManager());
  }
}
