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

package org.apache.samza.test.table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.LongSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.storage.kv.RocksDbTableProviderFactory;
import org.apache.samza.table.TableConfigGenerator;
import org.apache.samza.table.TableDescriptorsProvider;
import org.apache.samza.table.remote.RemoteTableDescriptor;
import org.apache.samza.table.remote.RemoteTableProviderFactory;
import org.apache.samza.table.remote.TableReadFunction;

import org.apache.samza.util.RateLimiter;
import org.apache.samza.util.Util;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;


/**
 * Table descriptors provider tests for both remote and local tables
 */
public class TestTableDescriptorsProvider {

  @Test
  public void testWithNoConfiguredTableDescriptorProviderClass() throws Exception {
    Map<String, String> configs = new HashMap<>();
    String tableRewriterName = "tableRewriter";
    Config resultConfig = new MySampleTableConfigRewriter().rewrite(tableRewriterName, new MapConfig(configs));
    Assert.assertTrue(resultConfig.size() == 0);
  }

  @Test
  public void testWithNonTableDescriptorsProviderClass() throws Exception {
    Map<String, String> configs = new HashMap<>();
    String tableRewriterName = "tableRewriter";
    configs.put("tables.descriptors.provider.class", MySampleNonTableDescriptorsProvider.class.getName());
    Config resultConfig = new MySampleTableConfigRewriter().rewrite(tableRewriterName, new MapConfig(configs));
    Assert.assertTrue(resultConfig.size() == 1);
    JavaTableConfig tableConfig = new JavaTableConfig(resultConfig);
    Assert.assertTrue(tableConfig.getTableIds().size() == 0);
  }

  @Test
  public void testWithTableDescriptorsProviderClass() throws Exception {
    Map<String, String> configs = new HashMap<>();
    String tableRewriterName = "tableRewriter";
    configs.put("tables.descriptors.provider.class", MySampleTableDescriptorsProvider.class.getName());
    Config resultConfig = new MySampleTableConfigRewriter().rewrite(tableRewriterName, new MapConfig(configs));
    Assert.assertTrue(resultConfig.size() == 17);

    String localTableId = "local-table-1";
    String remoteTableId = "remote-table-1";

    JavaStorageConfig storageConfig = new JavaStorageConfig(resultConfig);
    Assert.assertTrue(storageConfig.getStoreNames().size() == 1);
    Assert.assertEquals(storageConfig.getStoreNames().get(0), localTableId);
    Assert.assertEquals(storageConfig.getStorageFactoryClassName(localTableId),
        RocksDbKeyValueStorageEngineFactory.class.getName());
    Assert.assertTrue(storageConfig.getStorageKeySerde(localTableId).startsWith("StringSerde"));
    Assert.assertTrue(storageConfig.getStorageMsgSerde(localTableId).startsWith("StringSerde"));
    Config storeConfig = resultConfig.subset("stores." + localTableId + ".", true);
    Assert.assertTrue(storeConfig.size() == 4);
    Assert.assertEquals(storeConfig.getInt("rocksdb.block.size.bytes"), 4096);

    JavaTableConfig tableConfig = new JavaTableConfig(resultConfig);
    Assert.assertEquals(tableConfig.getTableProviderFactory(localTableId),
        RocksDbTableProviderFactory.class.getName());
    Assert.assertEquals(tableConfig.getTableProviderFactory(remoteTableId),
        RemoteTableProviderFactory.class.getName());
    Assert.assertTrue(tableConfig.getKeySerde(localTableId).startsWith("StringSerde"));
    Assert.assertTrue(tableConfig.getValueSerde(localTableId).startsWith("StringSerde"));
    Assert.assertTrue(tableConfig.getKeySerde(remoteTableId).startsWith("StringSerde"));
    Assert.assertTrue(tableConfig.getValueSerde(remoteTableId).startsWith("LongSerde"));
    Assert.assertEquals(tableConfig.getTableProviderFactory(localTableId), RocksDbTableProviderFactory.class.getName());
    Assert.assertEquals(tableConfig.getTableProviderFactory(remoteTableId), RemoteTableProviderFactory.class.getName());
  }

  public static class MySampleNonTableDescriptorsProvider {
  }

  public static class MySampleTableDescriptorsProvider implements TableDescriptorsProvider {
    @Override
    public List<TableDescriptor> getTableDescriptors(Config config) {
      List<TableDescriptor> tableDescriptors = new ArrayList<>();
      final RateLimiter readRateLimiter = mock(RateLimiter.class);
      final TableReadFunction readRemoteTable = (TableReadFunction) key -> null;

      tableDescriptors.add(new RemoteTableDescriptor<>("remote-table-1")
          .withReadFunction(readRemoteTable)
          .withRateLimiter(readRateLimiter, null, null)
          .withSerde(KVSerde.of(new StringSerde(), new LongSerde())));
      tableDescriptors.add(new RocksDbTableDescriptor("local-table-1")
          .withBlockSize(4096)
          .withSerde(KVSerde.of(new StringSerde(), new StringSerde())));
      return tableDescriptors;
    }
  }

  /**
   * A sample config rewriter to generate table configs. It instantiates the configured tableDescriptorsProvider class
   * which implements {@link TableDescriptorsProvider} and generates the table configs.
   */
  public static class MySampleTableConfigRewriter implements ConfigRewriter {

    @Override
    public Config rewrite(String name, Config config) {
      String tableDescriptorsProviderClassName = config.get("tables.descriptors.provider.class");
      if (tableDescriptorsProviderClassName == null || tableDescriptorsProviderClassName.isEmpty()) {
        // tableDescriptorsProviderClass is not configured
        return config;
      }

      try {
        if (!TableDescriptorsProvider.class.isAssignableFrom(Class.forName(tableDescriptorsProviderClassName))) {
          // The configured class does not implement TableDescriptorsProvider.
          return config;
        }

        TableDescriptorsProvider tableDescriptorsProvider =
            Util.getObj(tableDescriptorsProviderClassName, TableDescriptorsProvider.class);
        List<TableDescriptor> tableDescs = tableDescriptorsProvider.getTableDescriptors(config);
        return new MapConfig(Arrays.asList(config, TableConfigGenerator.generateConfigsForTableDescs(tableDescs)));
      } catch (Exception e) {
        throw new ConfigException(String.format("Invalid configuration for TableDescriptorsProvider class: %s",
            tableDescriptorsProviderClassName), e);
      }
    }
  }
}
