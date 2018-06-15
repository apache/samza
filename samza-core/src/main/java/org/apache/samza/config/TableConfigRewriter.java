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

package org.apache.samza.config;

import java.util.Arrays;
import java.util.List;

import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.table.TableConfigGenerator;
import org.apache.samza.table.TableDescriptorsFactory;
import org.apache.samza.util.Util;


/**
 * Config rewriter to generate table configs, to be used ONLY by low-level Task API using Samza tables. It instantiates
 * the configured task which implements {@link TableDescriptorsFactory} and generates the table configs. Please take a
 * look at {@link TableDescriptorsFactory} to get more details on how to use Samza tables.
 */
public class TableConfigRewriter implements ConfigRewriter {

  @Override
  public Config rewrite(String name, Config config) {
    TaskConfig taskConfig = new TaskConfig(config);
    if (taskConfig.getTaskClass().isEmpty()) {
      // taskClassName is not configured for high-level API.
      return config;
    }

    String taskClassName = taskConfig.getTaskClass().get();
    try {
      if (!TableDescriptorsFactory.class.isAssignableFrom(Class.forName(taskClassName))) {
        // The configured task does not implement TableDescriptorsFactory.
        return config;
      }

      TableDescriptorsFactory tableDescriptorsFactory = Util.getObj(taskClassName, TableDescriptorsFactory.class);
      List<TableDescriptor> tableDescs = tableDescriptorsFactory.getTableDescriptors();
      return new MapConfig(Arrays.asList(config, new TableConfigGenerator().generateConfigsForTableDescs(tableDescs)));
    } catch (Throwable t) {
      throw new ConfigException(String.format("Invalid configuration for Task class: %s", taskClassName), t);
    }
  }
}
