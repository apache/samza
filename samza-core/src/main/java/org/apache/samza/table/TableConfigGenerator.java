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

import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSerializerConfig;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.table.descriptors.LocalTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to generate table configs.
 */
public class TableConfigGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(TableConfigGenerator.class);

  /**
   * Generate configuration for provided tables
   *
   * @param jobConfig existing job config
   * @param tableDescriptors table descriptors, for which configuration to be generated
   * @return table configuration
   */
  public static Map<String, String> generate(Config jobConfig, List<TableDescriptor> tableDescriptors) {
    Map<String, String> tableConfig = new HashMap<>();
    tableDescriptors.forEach(tableDescriptor -> tableConfig.putAll(tableDescriptor.toConfig(jobConfig)));
    LOG.info("TableConfigGenerator has generated configs {}", tableConfig);
    return tableConfig;
  }

  /**
   * Generate serde configuration for provided tables
   *
   * @param tableDescriptors table descriptors, for which serde configuration to be generated
   * @return serde configuration for tables
   */
  public static Map<String, String> generateSerdeConfig(List<TableDescriptor> tableDescriptors) {

    Map<String, String> serdeConfigs = new HashMap<>();

    // Collect key and msg serde instances for all the tables
    Map<String, Serde> tableKeySerdes = new HashMap<>();
    Map<String, Serde> tableValueSerdes = new HashMap<>();
    HashSet<Serde> serdes = new HashSet<>();
    tableDescriptors.stream()
        .filter(d -> d instanceof LocalTableDescriptor)
        .forEach(d -> {
            LocalTableDescriptor ld = (LocalTableDescriptor) d;
            tableKeySerdes.put(ld.getTableId(), ld.getSerde().getKeySerde());
            tableValueSerdes.put(ld.getTableId(), ld.getSerde().getValueSerde());
          });
    serdes.addAll(tableKeySerdes.values());
    serdes.addAll(tableValueSerdes.values());

    // Generate serde names
    SerializableSerde<Serde> serializableSerde = new SerializableSerde<>();
    Base64.Encoder base64Encoder = Base64.getEncoder();
    Map<Serde, String> serdeUUIDs = new HashMap<>();
    serdes.forEach(serde -> {
        String serdeName = serdeUUIDs.computeIfAbsent(serde,
            s -> serde.getClass().getSimpleName() + "-" + UUID.randomUUID().toString());
        serdeConfigs.putIfAbsent(String.format(JavaSerializerConfig.SERDE_SERIALIZED_INSTANCE, serdeName),
            base64Encoder.encodeToString(serializableSerde.toBytes(serde)));
      });

    // Set key and msg serdes for tables to the serde names generated above
    tableKeySerdes.forEach((tableId, serde) -> {
        String keySerdeConfigKey = String.format(JavaTableConfig.STORE_KEY_SERDE, tableId);
        serdeConfigs.put(keySerdeConfigKey, serdeUUIDs.get(serde));
      });
    tableValueSerdes.forEach((tableId, serde) -> {
        String valueSerdeConfigKey = String.format(JavaTableConfig.STORE_MSG_SERDE, tableId);
        serdeConfigs.put(valueSerdeConfigKey, serdeUUIDs.get(serde));
      });
    return serdeConfigs;
  }
}
