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

package org.apache.samza.sql.data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.udfs.ScalarUdf;
import org.apache.samza.util.ReflectionUtil;


public class SamzaSqlExecutionContext implements Cloneable {

  /**
   * The variables that are shared among all cloned instance of {@link SamzaSqlExecutionContext}
   */
  private final SamzaSqlApplicationConfig sqlConfig;

  // Maps the UDF name to list of all UDF methods associated with the name.
  // Since we support polymorphism there can be multiple udfMetadata associated with the single name.
  private final Map<String, List<UdfMetadata>> udfMetadata;

  /**
   * The variable that are not shared among all cloned instance of {@link SamzaSqlExecutionContext}
   */
  private final Map<String, ScalarUdf> udfInstances = new HashMap<>();

  private SamzaSqlExecutionContext(SamzaSqlExecutionContext other) {
    this.sqlConfig = other.sqlConfig;
    this.udfMetadata = other.udfMetadata;
  }

  public SamzaSqlExecutionContext(SamzaSqlApplicationConfig config) {
    this.sqlConfig = config;
    udfMetadata = new HashMap<>();
    for(UdfMetadata udf : this.sqlConfig.getUdfMetadata()) {
      udfMetadata.putIfAbsent(udf.getName(), new ArrayList<>());
      udfMetadata.get(udf.getName()).add(udf);
    }
  }

  public ScalarUdf getOrCreateUdf(String clazz, String udfName) {
    return udfInstances.computeIfAbsent(udfName, s -> createInstance(clazz, udfName));
  }

  public ScalarUdf createInstance(String clazz, String udfName) {

    // Configs should be same for all the UDF methods within a UDF. Hence taking the first one.
    Config udfConfig = udfMetadata.get(udfName).get(0).getUdfConfig();
    ScalarUdf scalarUdf = ReflectionUtil.createInstanceOrNull(getClass().getClassLoader(), clazz, ScalarUdf.class);
    if (scalarUdf == null) {
      String msg = String.format("Couldn't create udf %s of class %s", udfName, clazz);
      throw new SamzaException(msg);
    }
    scalarUdf.init(udfConfig);
    return scalarUdf;
  }

  public SamzaSqlApplicationConfig getSamzaSqlApplicationConfig() {
    return sqlConfig;
  }

  @Override
  public SamzaSqlExecutionContext clone() {
    return new SamzaSqlExecutionContext(this);
  }

}
