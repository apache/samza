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
package org.apache.samza.monitor;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;


/**
 * Configurations for the monitor {@link LocalStoreMonitor}.
 */
public class LocalStoreMonitorConfig extends MapConfig {

  /**
   * Defines the local store directory of the job.
   */
  static final String CONFIG_LOCAL_STORE_DIR = "job.local.store.dir";

  /**
   * Defines the ttl of the offset file in milliseconds.
   * This must not be larger than delete.retention.ms(slightly lower is better).
   * For instance, if the delete.retention.ms is 24 hrs, this should be set to 23.5 hrs.
   */
  private static final String CONFIG_OFFSET_FILE_TTL = "job.offset.ttl.ms";

  /**
   * Defines the comma separated list of job status servers of the form
   * "Host1:Port1,Host2:Port2".
   */
  private static final String CONFIG_JOB_STATUS_SERVERS = "job.status.servers";

  /**
   * Default offset file ttl in milliseconds. Equivalent to 7 days.
   */
  private static final long DEFAULT_OFFSET_FILE_TTL_MS = 1000 * 60 * 60 * 24 * 7;

  public LocalStoreMonitorConfig(Config config) {
    super(config);
  }

  /**
   *
   * @return the location of the job's local directory.
   */
  public String getLocalStoreBaseDir() {
    return get(CONFIG_LOCAL_STORE_DIR);
  }

  /**
   *
   * @return the ttl of the offset file. Maximum age allowed for a ttl file.
   */
  public long getOffsetFileTTL() {
    return getLong(CONFIG_OFFSET_FILE_TTL, DEFAULT_OFFSET_FILE_TTL_MS);
  }

  /**
   *
   * @return a list of the job status servers in the form of host:port,
   *         these nodes will be tried in that order to access the rest apis hosted
   *         on the job status server.
   */
  public List<String> getJobStatusServers() {
     return Arrays.asList(StringUtils.split(get(CONFIG_JOB_STATUS_SERVERS), '.'));
  }
}
