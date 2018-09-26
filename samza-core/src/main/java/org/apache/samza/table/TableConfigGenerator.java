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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to generate table configs.
 */
public class TableConfigGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(TableConfigGenerator.class);

  /**
   * Generate table configurations given a list of table descriptors
   * @param config the job configuration
   * @param tableDescriptors the list of tableDescriptors
   * @return configuration for the tables
   */
  static public Map<String, String> generateConfigsForTableDescs(Config config, List<TableDescriptor> tableDescriptors) {
    return generateConfigsForTableSpecs(config, getTableSpecs(tableDescriptors));
  }

  /**
   * Generate table configurations given a list of table specs
   * @param config the job configuration
   * @param tableSpecs the list of tableSpecs
   * @return configuration for the tables
   */
  static public Map<String, String> generateConfigsForTableSpecs(Config config, List<TableSpec> tableSpecs) {
    Map<String, String> tableConfigs = new HashMap<>();

    tableSpecs.forEach(tableSpec -> {
        // Add table provider factory config
        tableConfigs.put(String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, tableSpec.getId()),
            tableSpec.getTableProviderFactoryClassName());

        // Generate additional configuration
        TableProviderFactory tableProviderFactory =
            Util.getObj(tableSpec.getTableProviderFactoryClassName(), TableProviderFactory.class);
        TableProvider tableProvider = tableProviderFactory.getTableProvider(tableSpec);
        tableConfigs.putAll(tableProvider.generateConfig(config, tableConfigs));
      });

    LOG.info("TableConfigGenerator has generated configs {}", tableConfigs);
    return tableConfigs;
  }

  /**
   * Get list of table specs given a list of table descriptors.
   * @param tableDescs the list of tableDescriptors
   * @return list of tableSpecs
   */
  static public List<TableSpec> getTableSpecs(List<TableDescriptor> tableDescs) {
    Map<TableSpec, TableImpl> tableSpecs = new LinkedHashMap<>();

    tableDescs.forEach(tableDesc -> {
        TableSpec tableSpec = ((BaseTableDescriptor) tableDesc).getTableSpec();

        if (tableSpecs.containsKey(tableSpec)) {
          throw new IllegalStateException(
              String.format("getTable() invoked multiple times with the same tableId: %s", tableDesc.getTableId()));
        }
        tableSpecs.put(tableSpec, new TableImpl(tableSpec));
      });
    return new ArrayList<>(tableSpecs.keySet());
  }
}
