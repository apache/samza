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
package org.apache.samza.sql.udf;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.interfaces.UdfResolver;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.udfs.SamzaSqlUdf;
import org.apache.samza.sql.udfs.SamzaSqlUdfMethod;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An UDF resolver implementation that uses reflection to discover the subtypes
 * of the {@link SamzaSqlUdf} from the classpath. Performs the validation to
 * ensure that all subtypes of {@link SamzaSqlUdf} extend and implement the
 * method annotated with {@link SamzaSqlUdfMethod}.
 */
public class ReflectionBasedUdfResolver implements UdfResolver {

  private static final Logger LOG = LoggerFactory.getLogger(ReflectionBasedUdfResolver.class);

  private static final String CONFIG_PACKAGE_PREFIX = "samza.sql.udf.resolver.package.prefix";

  private final Set<UdfMetadata> udfs  = new HashSet<>();

  public ReflectionBasedUdfResolver(Config udfConfig) {
    // Searching the entire classpath to discover the subtypes of SamzaSqlUdf is expensive. To reduce the search space,
    // the search is limited to the set of package prefixes defined in the configuration.
    // Within Linkedin this configuration will be overridden to ["com.linkedin.samza", "org.apache.samza", "com.linkedin.samza.sql.shade.prefix"].
    String samzaSqlUdfPackagePrefix = udfConfig.getOrDefault(CONFIG_PACKAGE_PREFIX, "org.apache.samza");

    // 1. Build the reflections instance  with appropriate configuration.
    ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
    configurationBuilder.forPackages(samzaSqlUdfPackagePrefix.split(","));
    configurationBuilder.addClassLoader(Thread.currentThread().getContextClassLoader());
    Reflections reflections = new Reflections(configurationBuilder);

    // 2. Get all the sub-types of SamzaSqlUdf.
    Set<Class<?>> typesAnnotatedWithSamzaSqlUdf = reflections.getTypesAnnotatedWith(SamzaSqlUdf.class);

    for (Class<?> udfClass : typesAnnotatedWithSamzaSqlUdf) {
      // 3. Get all the methods that are annotated with SamzaSqlUdfMethod
      List<Method> methodsAnnotatedWithSamzaSqlMethod = MethodUtils.getMethodsListWithAnnotation(udfClass, SamzaSqlUdfMethod.class);

      if (methodsAnnotatedWithSamzaSqlMethod.isEmpty()) {
        String msg = String.format("Udf class: %s doesn't have any methods annotated with: %s", udfClass.getName(), SamzaSqlUdfMethod.class.getName());
        LOG.error(msg);
        throw new SamzaException(msg);
      }

      SamzaSqlUdf sqlUdf = udfClass.getAnnotation(SamzaSqlUdf.class);
      // 4. If the udf is enabled, then add the udf information of the methods to the udfs list.
      if (sqlUdf.enabled()) {
        String udfName = sqlUdf.name();
        methodsAnnotatedWithSamzaSqlMethod.forEach(method -> {
          SamzaSqlUdfMethod samzaSqlUdfMethod = method.getAnnotation(SamzaSqlUdfMethod.class);
          List<SamzaSqlFieldType> params = Arrays.asList(samzaSqlUdfMethod.params());
          udfs.add(new UdfMetadata(udfName, sqlUdf.description(), method, udfConfig.subset(udfName + "."), params,
                  samzaSqlUdfMethod.returns(), samzaSqlUdfMethod.disableArgumentCheck()));
        });
      }
    }
  }

  @Override
  public Collection<UdfMetadata> getUdfs() {
    return udfs;
  }
}
