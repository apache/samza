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

import java.util.*;

/**
 * Specification of a EnvironmentVariableHandler. It specifies valid enviable variable names, possible values,
 * the default value, etc.
 */
public class EnvironmentVariableSpecs {
  private Map<String, Spec> specMap;

  /**
   * @param specMap A map of environment variable name vs its specification
   */
  public EnvironmentVariableSpecs(Map<String, Spec> specMap) {
    this.specMap = specMap;
  }

  /**
   * Get the Spec object of an environment variable
   * @param name name of the environment variable
   * @return A Spec object describing the specification of the environment variable
   */
  public Spec getSpec(String name) {
    return specMap.get(name);
  }

  /**
   * Gets all supported environment variables and their Specs.
   * @return A list of Pair containing the name and Spec of all environment variable.
   */
  public List<Pair<String, Spec>> getAllSpecs() {
    List<Pair<String, Spec>> list = new ArrayList<>();
    Iterator<Map.Entry<String, Spec>> it =  specMap.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry<String, Spec> entry = it.next();
      list.add(new Pair<>(entry.getKey(), entry.getValue()));
    }
    return list;
  }

  /**
   * Describes the specification of an environment variable.
   */
  public static class Spec {
    private String[] possibleValues;
    private String defaultValue;

    /**
     * @param possibleValues A list of possible valid values. Pass null or zero-length if there's no limitation.
     * @param defaultValue The default value of the environment variable
     */
    public Spec(String[] possibleValues, String defaultValue) {
      this.possibleValues = possibleValues;
      this.defaultValue = defaultValue;
    }

    /**
     * @return Possible valid values of the environment variable. Returns null or a zero-length array if
     * any value is acceptable.
     */
    public String[] getPossibleValues() {
      return possibleValues;
    }

    /**
     * @return Default value of the environment variable.
     */
    public String getDefaultValue() {
      return defaultValue;
    }
  }
}
