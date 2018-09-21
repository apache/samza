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
import org.apache.samza.config.JobConfig;
import org.apache.samza.system.inmemory.InMemorySystemFactory;


/**
 * CollectionStreamSystem represents a system that interacts with an underlying {@link InMemorySystemFactory} to create
 * various input and output streams and initialize {@link org.apache.samza.system.SystemStreamPartition} with messages
 * <p>
 * Following system level configs are set by default
 * <ol>
 *   <li>"systems.%s.default.stream.samza.offset.default" = "oldest"</li>
 *   <li>"jobs.job-name-and-id.systems.%s.default.stream.samza.offset.default" = "oldest"</li>
 *   <li>"systems.%s.samza.factory" = {@link InMemorySystemFactory}</li>
 *   <li>"jobs.job-name-and-id.systems.%s.samza.factory" = {@link InMemorySystemFactory}</li>
 * </ol>
 * The "systems.*" configs are required since the planner uses the system to get metadata about streams during
 * planning. The "jobs.job-name-and-id.systems.*" configs are required since configs generated from user provided
 * system/stream descriptors override configs originally supplied to the planner. Configs in the "jobs.job-name-and-id.*"
 * scope have the highest precedence.
 */
public class CollectionStreamSystemSpec {
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
   * @param jobNameAndId the job name and ID
   */
  private CollectionStreamSystemSpec(String systemName, String jobNameAndId) {
    this.systemName = systemName;
    systemConfigs = new HashMap<String, String>();
    systemConfigs.put(String.format(SYSTEM_FACTORY, systemName), InMemorySystemFactory.class.getName());
    systemConfigs.put(String.format(JobConfig.CONFIG_OVERRIDE_JOBS_PREFIX() + SYSTEM_FACTORY, jobNameAndId, systemName), InMemorySystemFactory.class.getName());
    systemConfigs.put(String.format(SYSTEM_OFFSET, systemName), "oldest");
    systemConfigs.put(String.format(JobConfig.CONFIG_OVERRIDE_JOBS_PREFIX() + SYSTEM_OFFSET, jobNameAndId, systemName), "oldest");
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
   * @param jobNameAndId the job name and ID
   * @return an instance of {@link CollectionStreamSystemSpec}
   */
  public static CollectionStreamSystemSpec create(String systemName, String jobNameAndId) {
    Preconditions.checkState(StringUtils.isNotBlank(systemName));
    Preconditions.checkState(StringUtils.isNotBlank(jobNameAndId));
    return new CollectionStreamSystemSpec(systemName, jobNameAndId);
  }
}

