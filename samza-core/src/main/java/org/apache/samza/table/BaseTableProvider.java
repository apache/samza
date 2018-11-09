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
package org.apache.samza.table;

import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Base class for all table provider implementations.
 */
abstract public class BaseTableProvider implements TableProvider {

  final protected Logger logger = LoggerFactory.getLogger(getClass());

  final protected TableSpec tableSpec;

  protected Context context;

  public BaseTableProvider(TableSpec tableSpec) {
    this.tableSpec = tableSpec;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init(Context context) {
    this.context = context;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, String> generateConfig(Config jobConfig, Map<String, String> generatedConfig) {
    Map<String, String> tableConfig = new HashMap<>();

    // Insert table_id prefix to config entries
    tableSpec.getConfig().forEach((k, v) -> {
        String realKey = String.format(JavaTableConfig.TABLE_ID_PREFIX, tableSpec.getId()) + "." + k;
        tableConfig.put(realKey, v);
      });

    logger.info("Generated configuration for table " + tableSpec.getId());

    return tableConfig;
  }

}
