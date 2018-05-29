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
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.operators.TableImpl;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.table.TableDescriptorFactory;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableProviderFactory;
import org.apache.samza.table.TableSpec;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class TableConfigRewriter implements ConfigRewriter {
  public static final String CFG_TABLE_DESCRIPTOR_FACTORY_CLASS = "job.config.rewriter.%s.table-descriptor-factory";
  public static final Logger LOGGER = LoggerFactory.getLogger(TableConfigRewriter.class);

  @Override
  public Config rewrite(String name, Config config) {
    String tableDescClass = config.get(String.format(CFG_TABLE_DESCRIPTOR_FACTORY_CLASS, name));
    TableDescriptorFactory tableDescriptorFactory = Util.getObj(tableDescClass, TableDescriptorFactory.class);
    TableDescriptor tableDescriptor = tableDescriptorFactory.getTableDescriptor();

    Map<String, String> configs = new HashMap<>();
    Set<TableSpec> tableSpecs = getTables(tableDescriptor);

    // collect all key and msg serde instances for tables
    Map<String, Serde> tableKeySerdes = new HashMap<>();
    Map<String, Serde> tableValueSerdes = new HashMap<>();
    HashSet<Serde> serdes = new HashSet<>();

    tableSpecs.forEach(tableSpec -> {
      tableKeySerdes.put(tableSpec.getId(), tableSpec.getSerde().getKeySerde());
      tableValueSerdes.put(tableSpec.getId(), tableSpec.getSerde().getValueSerde());
    });
    serdes.addAll(tableKeySerdes.values());
    serdes.addAll(tableValueSerdes.values());

    SerializableSerde<Serde> serializableSerde = new SerializableSerde<>();
    Base64.Encoder base64Encoder = Base64.getEncoder();
    Map<Serde, String> serdeUUIDs = new HashMap<>();
    serdes.forEach(serde -> {
      String serdeName = serdeUUIDs.computeIfAbsent(serde,
          s -> serde.getClass().getSimpleName() + "-" + UUID.randomUUID().toString());
      configs.putIfAbsent(String.format(SerializerConfig.SERDE_SERIALIZED_INSTANCE(), serdeName),
          base64Encoder.encodeToString(serializableSerde.toBytes(serde)));
    });

    // set key and msg serdes for tables to the serde names generated above
    tableKeySerdes.forEach((tableId, serde) -> {
      String keySerdeConfigKey = String.format(JavaTableConfig.TABLE_KEY_SERDE, tableId);
      configs.put(keySerdeConfigKey, serdeUUIDs.get(serde));
    });

    tableValueSerdes.forEach((tableId, serde) -> {
      String valueSerdeConfigKey = String.format(JavaTableConfig.TABLE_VALUE_SERDE, tableId);
      configs.put(valueSerdeConfigKey, serdeUUIDs.get(serde));
    });

    tableSpecs.forEach(tableSpec -> {
      // Table provider factory
      configs.put(String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, tableSpec.getId()),
          tableSpec.getTableProviderFactoryClassName());

      // Generate additional configuration
      TableProviderFactory tableProviderFactory =
          Util.getObj(tableSpec.getTableProviderFactoryClassName(), TableProviderFactory.class);
      TableProvider tableProvider = tableProviderFactory.getTableProvider(tableSpec);
      configs.putAll(tableProvider.generateConfig(configs));
    });

    return new MapConfig(Arrays.asList(config, configs));
  }

  public Set<TableSpec> getTables(TableDescriptor tableDesc) {
    Map<TableSpec, TableImpl> tables = new LinkedHashMap<>();
    TableSpec tableSpec = ((BaseTableDescriptor) tableDesc).getTableSpec();

    if (tables.containsKey(tableSpec)) {
      throw new IllegalStateException(String.format(
          "getTable() invoked multiple times with the same tableId: %s",
          tableDesc.getTableId()));
    }
    tables.put(tableSpec, new TableImpl(tableSpec));
    return new HashSet<>(tables.keySet());
  }
}
