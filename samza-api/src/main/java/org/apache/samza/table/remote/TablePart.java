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

package org.apache.samza.table.remote;

import java.util.Collections;
import java.util.Map;
import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;


/**
 * A building block of a remote table
 */
@InterfaceStability.Unstable
public interface TablePart {

  /**
   * Generate configuration for this building block. There are situations where this object
   * or its external dependencies may require certain configuration, this method allows
   * generation and inclusion of them in the job configuration.
   *
   * @param jobConfig job configuration
   * @param tableConfig so far generated configuration for this table
   * @return configuration for this build block
   */
  default Map<String, String> toConfig(Config jobConfig, Config tableConfig) {
    return Collections.emptyMap();
  }

}
