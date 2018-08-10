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

package org.apache.samza.sql.testutil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang.NotImplementedException;
import org.apache.samza.config.Config;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.sql.data.SamzaSqlCompositeKey;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.interfaces.SqlIOResolverFactory;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableProviderFactory;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.TaskContext;


public class TestIOResolverFactory implements SqlIOResolverFactory {
  public static final String TEST_DB_SYSTEM = "testDb";
  public static final String TEST_TABLE_ID = "testDbId";

  @Override
  public SqlIOResolver create(Config config) {
    return new TestIOResolver(config);
  }

  static class TestTableDescriptor extends BaseTableDescriptor {
    protected TestTableDescriptor(String tableId) {
      super(tableId);
    }

    @Override
    public String getTableId() {
      return tableId;
    }

    @Override
    public TableSpec getTableSpec() {
      return new TableSpec(tableId, KVSerde.of(new NoOpSerde(), new NoOpSerde()), TestTableProviderFactory.class.getName(), new HashMap<>());
    }
  }

  public static class TestTable implements ReadWriteTable {
    public static Map<Object, Object> records = new HashMap<>();
    @Override
    public Object get(Object key) {
      throw new NotImplementedException();
    }

    @Override
    public CompletableFuture getAsync(Object key) {
      throw new NotImplementedException();
    }

    @Override
    public Map getAll(List keys) {
      throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Map> getAllAsync(List keys) {
      throw new NotImplementedException();
    }

    @Override
    public void close() {
    }

    @Override
    public void put(Object key, Object value) {
      if (key == null) {
        records.put(System.nanoTime(), value);
      } else if (value != null) {
        records.put(key, value);
      } else {
        delete(key);
      }
    }

    @Override
    public CompletableFuture<Void> putAsync(Object key, Object value) {
      throw new NotImplementedException();
    }

    @Override
    public CompletableFuture<Void> putAllAsync(List list) {
      throw new NotImplementedException();
    }

    @Override
    public void delete(Object key) {
      records.remove(key);
    }

    @Override
    public CompletableFuture<Void> deleteAsync(Object key) {
      throw new NotImplementedException();
    }

    @Override
    public void deleteAll(List keys) {
      records.clear();
    }

    @Override
    public CompletableFuture<Void> deleteAllAsync(List keys) {
      throw new NotImplementedException();
    }

    @Override
    public void flush() {
    }

    @Override
    public void putAll(List entries) {
      throw new NotImplementedException();
    }
  }

  public static class TestTableProviderFactory implements TableProviderFactory {
    @Override
    public TableProvider getTableProvider(TableSpec tableSpec) {
      return new TestTableProvider();
    }
  }

  static class TestTableProvider implements TableProvider {
    @Override
    public void init(SamzaContainerContext containerContext, TaskContext taskContext) {
    }

    @Override
    public Table getTable() {
      return new TestTable();
    }

    @Override
    public Map<String, String> generateConfig(Map<String, String> config) {
      return new HashMap<>();
    }

    @Override
    public void close() {
    }
  }

  private class TestIOResolver implements SqlIOResolver {
    private final String SAMZA_SQL_QUERY_TABLE_KEYWORD = "$table";
    private final Config config;
    private final Map<String, TableDescriptor> tableDescMap = new HashMap<>();

    public TestIOResolver(Config config) {
      this.config = config;
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
            tableDescriptor = new TestTableDescriptor(TEST_TABLE_ID + tableDescMap.size());
          } else {
            tableDescriptor = new RocksDbTableDescriptor("InputTable-" + ioName)
                .withSerde(KVSerde.of(
                    new JsonSerdeV2<>(SamzaSqlCompositeKey.class),
                    new JsonSerdeV2<>(SamzaSqlRelMessage.class)));
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
