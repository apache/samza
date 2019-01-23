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

package org.apache.samza.sql.impl;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.interfaces.UdfResolver;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.apache.samza.sql.udfs.ScalarUdf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Udf resolver that uses static config to return the UDFs present in the Samza SQL application
 * All the UDF classes are provided to this factory as a comma separated list of values for the config named
 * "udfClasses".
 * This factory loads all the udf classes that are configured, performs the validation to ensure that they extend
 * {@link ScalarUdf} and implement the method named "execute"
 */
public class ConfigBasedUdfResolver implements UdfResolver {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigBasedUdfResolver.class);
  public static final String CFG_UDF_CLASSES = "udfClasses";

  private final ArrayList<UdfMetadata> udfs;

  public ConfigBasedUdfResolver(Properties config, Config udfConfig) {
    List<String> udfClasses = Arrays.stream(config.getProperty(CFG_UDF_CLASSES, "").split(","))
        .filter(StringUtils::isNotBlank)
        .collect(Collectors.toList());
    udfs = new ArrayList<>();
    Class<?> udfClass;
    for (String udfClassName : udfClasses) {
      try {
        udfClass = Class.forName(udfClassName);
      } catch (ClassNotFoundException e) {
        String msg = String.format("Couldn't load the udf class %s", udfClassName);
        LOG.error(msg, e);
        throw new SamzaException(msg, e);
      }

      if (!ScalarUdf.class.isAssignableFrom(udfClass)) {
        String msg = String.format("Udf class %s is not extended from %s", udfClassName, ScalarUdf.class.getName());
        LOG.error(msg);
        throw new SamzaException(msg);
      }

      SamzaSqlUdf sqlUdf;
      SamzaSqlUdfMethod sqlUdfMethod = null;
      Method udfMethod = null;

      sqlUdf = udfClass.getAnnotation(SamzaSqlUdf.class);
      Method[] methods = udfClass.getMethods();
      for (Method method : methods) {
        sqlUdfMethod = method.getAnnotation(SamzaSqlUdfMethod.class);
        if (sqlUdfMethod != null) {
          udfMethod = method;
          break;
        }
      }

      if (sqlUdf == null) {
        String msg = String.format("UdfClass %s is not annotated with SamzaSqlUdf", udfClass);
        LOG.error(msg);
        throw new SamzaException(msg);
      }

      if (sqlUdfMethod == null) {
        String msg = String.format("UdfClass %s doesn't have any methods annotated with SamzaSqlUdfMethod", udfClass);
        LOG.error(msg);
        throw new SamzaException(msg);
      }

      if (sqlUdf.enabled()) {
        String udfName = sqlUdf.name();
        udfs.add(new UdfMetadata(udfName, udfMethod, udfConfig.subset(udfName + ".")));
      }
    }
  }

  @Override
  public Collection<UdfMetadata> getUdfs() {
    return udfs;
  }
}
