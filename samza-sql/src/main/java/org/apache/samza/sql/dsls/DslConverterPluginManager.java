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

package org.apache.samza.sql.dsls;

import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.dsls.samzasql.SamzaSqlDslConverterFactory;
import org.apache.samza.sql.interfaces.DslConverter;
import org.apache.samza.sql.interfaces.DslConverterFactory;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Dsl converter plugin manager creates the {@link DslConverter} based on the dsl file extension.
 */
public class DslConverterPluginManager {

  public static final String CFG_DSL_FORMAT_CONVERTER_FACTORY = "samza.sql.format.%s.dslConverterFactory";

  private static final Logger LOG = LoggerFactory.getLogger(DslConverterPluginManager.class);

  /**
   * Create dsl converter based on the file extension and the {@link DslConverterFactory} configured for that file
   * extension. If such configuration does not exist, the default
   * {@link org.apache.samza.sql.dsls.samzasql.SamzaSqlDslConverter} is created.
   *
   * @param config the config
   * @return the {@link DslConverter}
   */
  static public DslConverter create(Config config) {
    // Default dsl file extension is sql. The default is applied when the sqlFile config is not set.
    String dslFileExtension = "sql";

    if (config.containsKey(SamzaSqlApplicationConfig.CFG_SQL_FILE)) {
      String dslFilePath[] = config.get(SamzaSqlApplicationConfig.CFG_SQL_FILE).split("\\.");
      dslFileExtension = dslFilePath[dslFilePath.length - 1];
    }

    String dslFormatConfigName = String.format(CFG_DSL_FORMAT_CONVERTER_FACTORY, dslFileExtension);
    String dslConverterFactoryClass = config.get(dslFormatConfigName);

    if (dslConverterFactoryClass == null || dslConverterFactoryClass.isEmpty()) {
      // Default converter factory is SamzaSqlDslConverterFactory
      dslConverterFactoryClass = SamzaSqlDslConverterFactory.class.getName();
      LOG.warn("Config {} is not set. Hence, using the default factory {}", dslFormatConfigName,
          dslConverterFactoryClass);
    }

    LOG.info("Using dsl converter factory {} to create logical plan.", dslConverterFactoryClass);

    DslConverterFactory dslConverterFactory;
    try {
      Class<?> clazz = Class.forName(dslConverterFactoryClass);
      dslConverterFactory = (DslConverterFactory) clazz.newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new SamzaException("Unable to instantiate dsl converter factory.", e);
    }
    return dslConverterFactory.create(config);
  }
}
