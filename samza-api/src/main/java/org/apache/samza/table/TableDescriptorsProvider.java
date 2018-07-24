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

import java.util.List;

import org.apache.samza.annotation.InterfaceStability;
import org.apache.samza.config.Config;
import org.apache.samza.operators.TableDescriptor;


/**
 * Provider to create a list of {@link TableDescriptor} objects to describe one or more Samza tables. This is the
 * mechanism for providing table support for Samza low level API.
 *
 * Developers writing Samza jobs using Samza table(s) should describe the table(s) by implementing
 * TableDescriptorsProvider.
 *
 * Typical user code using Samza tables should look like the following:
 *
 * <pre>
 * {@code
 * public class SampleTableDescriptorsProvider implements TableDescriptorsProvider {
 *   private ReadableTable<String, Long> remoteTable;
 *   private ReadWriteTable<String, String> localTable;
 *
 *   {@code @Override}
 *   public List<TableDescriptor> getTableDescriptors() {
 *     List<TableDescriptor> tableDescriptors = new ArrayList<>();
 *     final TableReadFunction readRemoteTableFn = new MyStoreReadFunction();
 *     tableDescriptors.add(new RemoteTableDescriptor<>("remote-table-1")
 *       .withReadFunction(readRemoteTableFn)
 *       .withSerde(KVSerde.of(new StringSerde(), new StringSerde())));
 *
 *     tableDescriptors.add(new RocksDbTableDescriptor("local-table-1")
 *       .withBlockSize(4096)
 *       .withSerde(KVSerde.of(new LongSerde(), new StringSerde<>())));
 *       .withConfig("some-key", "some-value");
 *     return tableDescriptors;
 *   }
 * }
 * }
 * </pre>
 *
 * [TODO:SAMZA-1772] will complete the work of introducing low-level Table API. Until then, Table API in low-level
 * could be used by generating configs from TableDescriptorsProvider (sample code below) through config rewriter.
 *
 * <pre>
 * {@code
 * private Map<String, String> generateTableConfigs(Config config) {
 *   String tableDescriptorsProviderClassName = config.get("tables.descriptors.provider.class");
 *   if (tableDescriptorsProviderClassName == null || tableDescriptorsProviderClassName.isEmpty()) {
 *      // tableDescriptorsProviderClass is not configured
 *      return config;
 *   }
 *
 *   try {
 *      if (!TableDescriptorsProvider.class.isAssignableFrom(Class.forName(tableDescriptorsProviderClassName))) {
 *         LOG.warn("TableDescriptorsProvider class {} does not implement TableDescriptosProvider.",
 *            tableDescriptorsProviderClassName);
 *         return config;
 *      }
 *
 *      TableDescriptorsProvider tableDescriptorsProvider =
 *          Util.getObj(tableDescriptorsProviderClassName, TableDescriptorsProvider.class);
 *      List<TableDescriptor> tableDescs = tableDescriptorsProvider.getTableDescriptors(config);
 *      return TableConfigGenerator.generateConfigsForTableDescs(tableDescs);
 *   } catch (Exception e) {
 *      throw new ConfigException(String.format("Invalid configuration for TableDescriptorsProvider class: %s",
 *          tableDescriptorsProviderClassName), e);
 *   }
 * }
 * }
 * </pre>
 */
@InterfaceStability.Unstable
public interface TableDescriptorsProvider {
  /**
   * Constructs instances of the table descriptors
   * @param config
   * @return list of table descriptors
   */
  List<TableDescriptor> getTableDescriptors(Config config);
}
