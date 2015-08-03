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

package org.apache.samza.coordinator.stream.messages;

/**
 * A coordinator stream message that tells the job coordinator to set a
 * specific configuration.
 */
public class SetConfig extends CoordinatorStreamMessage {
  public static final String TYPE = "set-config";
  private static final String CONFIG_VALUE_KEY = "value";

  /**
   * The SetConfig message is used to store a specific configuration.
   * @param message message to store which holds the configuration.
   */
  public SetConfig(CoordinatorStreamMessage message) {
    super(message.getKeyArray(), message.getMessageMap());
  }

  /**
   * The SetConfig message is used to store a specific configuration.
   * This constructor is used to create a SetConfig message for a given source for a specific config key and config value.
   * @param source the source of the config
   * @param key the key for the given config
   * @param value the value for the given config
   */
  public SetConfig(String source, String key, String value) {
    super(source);
    setType(TYPE);
    setKey(key);
    putMessageValue(CONFIG_VALUE_KEY, value);
  }

  /**
   * Return the configuration value as a string.
   * @return the configuration as a string
   */
  public String getConfigValue() {
    return getMessageValue(CONFIG_VALUE_KEY);
  }
}
