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


/**
 * Dsl converter plugin manager creates the DslConverter based on the format of the dsl file.
 */
public class DslConverterPluginManager {

  /**
   * Create dsl converter.
   *
   * @param config the config
   * @return the dsl converter
   */
  static public DslConverter create(Config config) {
    // default dsl file format is sql.
    String dslFileFormat = "sql";

    if (config.containsKey(SamzaSqlApplicationConfig.CFG_SQL_FILE)) {
      String dslFilePath[] = config.get(SamzaSqlApplicationConfig.CFG_SQL_FILE).split("\\.");
      dslFileFormat = dslFilePath[dslFilePath.length - 1];
    }

    // default coverter factory is SamzaSqlDslConverterFactory
    String dslConverterFactoryClass =
        config.get(String.format(SamzaSqlApplicationConfig.CFG_DSL_FORMAT_FACTORY, dslFileFormat),
            SamzaSqlDslConverterFactory.class.getName());
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
