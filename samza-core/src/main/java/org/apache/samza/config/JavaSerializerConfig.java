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

import java.util.ArrayList;
import java.util.List;

/**
 * java version of the SerializerConfig
 */
public class JavaSerializerConfig extends MapConfig {
  private final static String SERIALIZER_PREFIX = "serializers.registry.%s";
  private final static String SERDE = "serializers.registry.%s.class";

  public JavaSerializerConfig(Config config) {
    super(config);
  }

  public String getSerdeClass(String name) {
    return get(String.format(SERDE, name), null);
  }

  /**
   * Useful for getting individual serializers.
   * @return a list of all serializer names from the config file
   */
  public List<String> getSerdeNames() {
    List<String> results = new ArrayList<String>();
    Config subConfig = subset(String.format(SERIALIZER_PREFIX, ""), true);
    for (String key : subConfig.keySet()) {
      if (key.endsWith(".class")) {
        results.add(key.replace(".class", ""));
      }
    }
    return results;
  }
}
