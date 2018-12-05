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

import org.apache.samza.sql.client.util.CliUtil;
import org.apache.samza.sql.client.util.Pair;

import java.util.*;

/**
 * Specification of a EnvironmentVariableHandler. It specifies valid enviable variable names, possible values,
 * the default value, etc.
 */
public class EnvironmentVariableSpecs {
  private Map<String, Spec> specMap = new HashMap<>();

  public EnvironmentVariableSpecs() {
  }

  /**
   * @param envName name of the environment variable
   * @param possibleValues Pass an empty array or null for unlimited possible values
   * @param defaultValue default value of the environment variable
   */
  public void put(String envName, String[] possibleValues, String defaultValue) {
    if(CliUtil.isNullOrEmpty(envName) || CliUtil.isNullOrEmpty(defaultValue))
      throw new IllegalArgumentException();

    specMap.put(envName, new Spec(possibleValues, defaultValue));
  }

  public Spec getSpec(String envName) {
    return specMap.get(envName);
  }

  public List<Pair<String, Spec>> getAllSpecs() {
    List<Pair<String, Spec>> list = new ArrayList<>();
    Iterator<Map.Entry<String, Spec>> it =  specMap.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry<String, Spec> entry = it.next();
      list.add(new Pair<>(entry.getKey(), entry.getValue()));
    }
    return list;
  }

  public class Spec {
    private String[] possibleValues;
    private String defaultValue;

    Spec(String[] possibleValues, String defaultValue) {
      this.possibleValues = possibleValues;
      this.defaultValue = defaultValue;
    }

    public String[] getPossibleValues() {
      return possibleValues;
    }

    public String getDefaultValue() {
      return defaultValue;
    }
  }
}
