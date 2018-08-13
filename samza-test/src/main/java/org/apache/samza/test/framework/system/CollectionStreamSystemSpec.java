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

import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.system.inmemory.InMemorySystemFactory;


/**
 * CollectionStreamSystem represents a system that interacts with an underlying {@link InMemorySystemFactory} to create
 * various input and output streams and initialize {@link org.apache.samza.system.SystemStreamPartition} with messages
 * <p>
 * Following system level configs are set by default
 * <ol>
 *   <li>"systems.%s.default.stream.samza.offset.default" = "oldest"</li>
 *   <li>"jobs.1.systems.%s.default.stream.samza.offset.default" = "oldest"</li>
 *   <li>"systems.%s.samza.factory" = {@link InMemorySystemFactory}</li>
 *   <li>"jobs.1.systems.%s.samza.factory" = {@link InMemorySystemFactory}</li>
 * </ol>
 *
 */
public class CollectionStreamSystemSpec {
  private static final String CONFIG_OVERRIDE_PREFIX = "jobs.%s.";
  private static final String SYSTEM_FACTORY = "systems.%s.samza.factory";
  private static final String SYSTEM_OFFSET = "systems.%s.default.stream.samza.offset.default";

  private String systemName;
  private Map<String, String> systemConfigs;

  /**
   * Constructs a new CollectionStreamSystem from specified components.
   * <p>
   * Every {@link CollectionStreamSystemSpec} is assumed to consume from the oldest offset, since stream is in memory and
   * is used for testing purpose. System uses {@link InMemorySystemFactory} to initialize in memory streams.
   * <p>
   * @param systemName represents unique name of the system
   */
  private CollectionStreamSystemSpec(String systemName, String jobName) {
    this.systemName = systemName;
    systemConfigs = new HashMap<String, String>();
    systemConfigs.put(String.format(SYSTEM_FACTORY, systemName), InMemorySystemFactory.class.getName());
    systemConfigs.put(String.format(CONFIG_OVERRIDE_PREFIX + SYSTEM_FACTORY, jobName, systemName), InMemorySystemFactory.class.getName());
    systemConfigs.put(String.format(SYSTEM_OFFSET, systemName), "oldest");
    systemConfigs.put(String.format(CONFIG_OVERRIDE_PREFIX + SYSTEM_OFFSET, jobName, systemName), "oldest");
  }

  public String getSystemName() {
    return systemName;
  }

  public Map<String, String> getSystemConfigs() {
    return systemConfigs;
  }

  /**
   * Creates a {@link CollectionStreamSystemSpec} with name {@code systemName}
   * @param systemName represents name of the {@link CollectionStreamSystemSpec}
   * @param jobName name of the job
   * @return an instance of {@link CollectionStreamSystemSpec}
   */
  public static CollectionStreamSystemSpec create(String systemName, String jobName) {
    Preconditions.checkState(StringUtils.isNotBlank(systemName));
    Preconditions.checkState(StringUtils.isNotBlank(jobName));
    return new CollectionStreamSystemSpec(systemName, jobName);
  }
}

