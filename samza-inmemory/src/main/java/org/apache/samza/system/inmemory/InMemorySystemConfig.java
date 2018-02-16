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

import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;


public class InMemorySystemConfig extends MapConfig {
  // Serialized data set to initialize the consumer
  private static final String SERIALIZED_DATA_SET = "streams.%s.dataset";
  private static final String SERIALIZED_DATA_SET_DEFAULT = "";

  public InMemorySystemConfig(Config config) {
    super(config);
  }

  public String getSerializedDataSet(String streamId) {
    return getOrDefault(String.format(SERIALIZED_DATA_SET, streamId), SERIALIZED_DATA_SET_DEFAULT);
  }
}
