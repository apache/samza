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

package org.apache.samza.sql.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.sql.serializers.SamzaSqlRelMessageSerdeFactory;
import org.apache.samza.sql.serializers.SamzaSqlRelRecordSerdeFactory;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.interfaces.SqlIOResolverFactory;
import org.apache.samza.storage.kv.descriptors.RocksDbTableDescriptor;
import org.apache.samza.table.remote.BaseTableFunction;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;

import static org.apache.samza.sql.runner.SamzaSqlApplicationConfig.CFG_METADATA_TOPIC_PREFIX;
import static org.apache.samza.sql.runner.SamzaSqlApplicationConfig.DEFAULT_METADATA_TOPIC_PREFIX;


public class RemoteStoreIOResolverTestFactory implements SqlIOResolverFactory {
  public static final String TEST_REMOTE_STORE_SYSTEM = "testRemoteStore";
  public static final String TEST_TABLE_ID = "testTableId";

  public static transient Map<Object, Object> records = new HashMap<>();

  @Override
  public SqlIOResolver create(Config config, Config fullConfig) {
    return new TestRemoteStoreIOResolver(config);
  }

  public static class InMemoryWriteFunction extends BaseTableFunction
      implements TableWriteFunction<Object, Object, Object> {

    @Override
    public CompletableFuture<Void> putAsync(Object key, Object record) {
      records.put(key.toString(), record);
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> updateAsync(Object key, @Nullable Object record) {
      throw new SamzaException("Update unsupported");
    }

    @Override
    public CompletableFuture<Void> deleteAsync(Object key) {
      records.remove(key.toString());
      return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }
  }

  static class InMemoryReadFunction extends BaseTableFunction
      implements TableReadFunction<Object, Object> {

    @Override
    public CompletableFuture<Object> getAsync(Object key) {
      return CompletableFuture.completedFuture(records.get(key.toString()));
    }

    @Override
    public boolean isRetriable(Throwable exception) {
      return false;
    }
  }

  private class TestRemoteStoreIOResolver implements SqlIOResolver {
    private static final String SAMZA_SQL_QUERY_TABLE_KEYWORD = "$table";
    private final Config config;
    private final Map<String, TableDescriptor> tableDescMap = new HashMap<>();
    private final String changeLogStorePrefix;

    public TestRemoteStoreIOResolver(Config config) {
      this.config = config;
      String metadataTopicPrefix = config.get(CFG_METADATA_TOPIC_PREFIX, DEFAULT_METADATA_TOPIC_PREFIX);
      this.changeLogStorePrefix = metadataTopicPrefix + (metadataTopicPrefix.isEmpty() ? "" : "_");
    }

    private SqlIOConfig fetchIOInfo(String ioName, boolean isSink) {
      String[] sourceComponents = ioName.split("\\.");
      int systemIdx = 0;
      int endIdx = sourceComponents.length - 1;
      int streamIdx = endIdx;
      TableDescriptor tableDescriptor = null;

      if (sourceComponents[endIdx].equalsIgnoreCase(SAMZA_SQL_QUERY_TABLE_KEYWORD)) {
        streamIdx = endIdx - 1;

        tableDescriptor = tableDescMap.get(ioName);

        if (tableDescriptor == null) {
          if (isSink) {
            tableDescriptor = new RemoteTableDescriptor<>(TEST_TABLE_ID + "-" + ioName.replace(".", "-").replace("$", "-"))
                .withReadFunction(new InMemoryReadFunction())
                .withWriteFunction(new InMemoryWriteFunction())
                .withRateLimiterDisabled();
          } else if (sourceComponents[systemIdx].equals(TEST_REMOTE_STORE_SYSTEM)) {
            tableDescriptor = new RemoteTableDescriptor<>(TEST_TABLE_ID + "-" + ioName.replace(".", "-").replace("$", "-"))
                .withReadFunction(new InMemoryReadFunction())
                .withRateLimiterDisabled();
          } else {
            // A local table
            String tableId = changeLogStorePrefix + "InputTable-" + ioName.replace(".", "-").replace("$", "-");
            SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde keySerde =
                (SamzaSqlRelRecordSerdeFactory.SamzaSqlRelRecordSerde) new SamzaSqlRelRecordSerdeFactory().getSerde(null, null);
            SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde valueSerde =
                (SamzaSqlRelMessageSerdeFactory.SamzaSqlRelMessageSerde) new SamzaSqlRelMessageSerdeFactory().getSerde(null, null);
            tableDescriptor = new RocksDbTableDescriptor(tableId, KVSerde.of(keySerde, valueSerde)).withChangelogEnabled();
          }
          tableDescMap.put(ioName, tableDescriptor);
        }
      }

      Config systemConfigs = config.subset(sourceComponents[systemIdx] + ".");
      return new SqlIOConfig(sourceComponents[systemIdx], sourceComponents[streamIdx],
          Arrays.asList(sourceComponents), systemConfigs, tableDescriptor);
    }

    @Override
    public SqlIOConfig fetchSourceInfo(String sourceName) {
      return fetchIOInfo(sourceName, false);
    }

    @Override
    public SqlIOConfig fetchSinkInfo(String sinkName) {
      return fetchIOInfo(sinkName, true);
    }
  }
}
