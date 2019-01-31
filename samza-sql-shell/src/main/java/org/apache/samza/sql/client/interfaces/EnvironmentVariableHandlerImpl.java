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

import org.apache.samza.sql.client.util.CliException;
import org.apache.samza.sql.client.util.Pair;

import java.util.*;

public abstract class EnvironmentVariableHandlerImpl implements EnvironmentVariableHandler{
  private EnvironmentVariableSpecs specs;
  protected Map<String, String> envVars = new HashMap<>();

  public EnvironmentVariableHandlerImpl() {
    this.specs = initializeEnvironmentVariableSpecs();
  }

  @Override
  public int setEnvironmentVariable(String name, String value) {
    EnvironmentVariableSpecs.Spec spec = specs.getSpec(name);
    if(spec == null) {
      if(isAcceptUnknowName()) {
        return setEnvironmentVariableHelper(name, value);
      } else {
        return -1;
      }
    }

    String[] possibleValues = spec.getPossibleValues();
    if (possibleValues == null || possibleValues.length == 0) {
      return setEnvironmentVariableHelper(name, value);
    }

    for (String s : possibleValues) {
      if (s.equalsIgnoreCase(value)) {
        if(!processEnvironmentVariable(name, value)) {
          throw new CliException(); // should not reach here
        }
        envVars.put(name, value);
        return 0;
      }
    }
    return -2;
  }

  @Override
  public String getEnvironmentVariable(String name) {
    return envVars.get(name);
  }

  @Override
  public List<Pair<String, String>> getAllEnvironmentVariables() {
    List<Pair<String, String>> list = new ArrayList<>();
    Iterator<Map.Entry<String, String>> it =  envVars.entrySet().iterator();
    while(it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      list.add(new Pair<>(entry.getKey(), entry.getValue()));
    }
    return list;
  }

  @Override
  public EnvironmentVariableSpecs getSpecs() {
    return specs;
  }

  /**
   * Subclasses shall override this method to process their settings
   * @param name name of the environment variable
   * @param value value of the environment variable
   * @return succeed or not
   */
  protected abstract boolean processEnvironmentVariable(String name, String value);

  /**
   * Subclasses shall override this method to provide with their EnvironmentVariableSpecs
   * @return An object of EnvironmentVariableSpecs
   */
  protected abstract EnvironmentVariableSpecs initializeEnvironmentVariableSpecs();

  /**
   * Subclasses can override this method if they want to accept environment variables
   * with unknown names. When an executor is not able to go through all the valid configurations
   * it can use this method as a temporary workaround.
   * @return true if environment variables with unknown names are acceptable. This method returns false by default.
   */
  protected boolean isAcceptUnknowName() {
    return false;
  }

  private int setEnvironmentVariableHelper(String name, String value) {
    if(processEnvironmentVariable(name, value)) {
      envVars.put(name, value);
      return 0;
    } else {
      return -2;
    }
  }
}
