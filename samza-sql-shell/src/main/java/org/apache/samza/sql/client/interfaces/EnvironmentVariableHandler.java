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

package org.apache.samza.sql.client.interfaces;

import org.apache.samza.sql.client.util.Pair;

import java.util.List;

/**
 * An executor, or shell itself, need to handle environment variables to have their behavior customizable.
 */
public interface EnvironmentVariableHandler {
  /**
   * @param envName Environment variable name
   * @param value Value of the environment variable
   * @return 0 : succeed
   * -1: invalid envName
   * -2: invalid value
   */
  public int setEnvironmentVariable(String envName, String value);

  /**
   * @param envName Environment variable name
   * @return value of environment variable envName. Returns null if envName doesn't exist.
   */
  public String getEnvironmentVariable(String envName);

  /**
   *
   * @return All environment variables and their value.
   */
  public List<Pair<String, String>> getAllEnvironmentVariables();

  /**
   *
   * @return The corresponding EnvironmentVariableSpec.
   */
  public EnvironmentVariableSpecs getSpecs();
}
