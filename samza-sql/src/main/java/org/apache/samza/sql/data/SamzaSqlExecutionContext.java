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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.testutil.ReflectionUtils;
import org.apache.samza.sql.udfs.ScalarUdf;


public class SamzaSqlExecutionContext implements Cloneable {

  /**
   * The variables that are shared among all cloned instance of {@link SamzaSqlExecutionContext}
   */
  private final SamzaSqlApplicationConfig sqlConfig;
  private final Map<String, UdfMetadata> udfMetadata;

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
    udfMetadata =
        this.sqlConfig.getUdfMetadata().stream().collect(Collectors.toMap(UdfMetadata::getName, Function.identity()));
  }

  public ScalarUdf getOrCreateUdf(String clazz, String udfName) {
    return udfInstances.computeIfAbsent(udfName, s -> createInstance(clazz, udfName));
  }

  public ScalarUdf createInstance(String clazz, String udfName) {
    Config udfConfig = udfMetadata.get(udfName).getUdfConfig();
    ScalarUdf scalarUdf = ReflectionUtils.createInstance(clazz);
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
