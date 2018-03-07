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

import org.apache.commons.lang.NotImplementedException;
import org.apache.samza.config.Config;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.operators.BaseTableDescriptor;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.sql.interfaces.SourceResolver;
import org.apache.samza.sql.interfaces.SourceResolverFactory;
import org.apache.samza.sql.interfaces.SqlSystemSourceConfig;
import org.apache.samza.table.ReadWriteTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableProviderFactory;
import org.apache.samza.table.TableSpec;
import org.apache.samza.task.TaskContext;


public class TestSourceResolverFactory implements SourceResolverFactory {
  public static final String TEST_DB_SYSTEM = "testDb";
  public static final String TEST_TABLE_ID = "testDbId";

  @Override
  public SourceResolver create(Config config) {
    return new TestSourceResolver(config);
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
    public Map getAll(List keys) {
      throw new NotImplementedException();
    }

    @Override
    public void close() {
    }

    @Override
    public void put(Object key, Object value) {
      if (key == null) {
        records.put(System.nanoTime(), value);
      } else {
        records.put(key, value);
      }
    }

    @Override
    public void delete(Object key) {
      records.remove(key);
    }

    @Override
    public void deleteAll(List keys) {
      records.clear();
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

  private class TestSourceResolver implements SourceResolver {
    private final String SAMZA_SQL_QUERY_TABLE_KEYWORD = "$table";
    private final Config config;

    public TestSourceResolver(Config config) {
      this.config = config;
    }

    @Override
    public SqlSystemSourceConfig fetchSourceInfo(String sourceName) {
      String[] sourceComponents = sourceName.split("\\.");
      boolean isTable = false;
      int systemIdx = 0;
      int endIdx = sourceComponents.length - 1;
      int streamIdx = endIdx;

      if (sourceComponents[endIdx].equalsIgnoreCase(SAMZA_SQL_QUERY_TABLE_KEYWORD)) {
        isTable = true;
        streamIdx = endIdx - 1;
      }

      Config systemConfigs = config.subset(sourceComponents[systemIdx] + ".");
      return new SqlSystemSourceConfig(sourceComponents[systemIdx], sourceComponents[streamIdx],
          Arrays.asList(sourceComponents), systemConfigs, isTable);
    }

    @Override
    public boolean isTable(String sourceName){
      String[] sourceComponents = sourceName.split("\\.");
      return sourceComponents[sourceComponents.length - 1].equalsIgnoreCase(SAMZA_SQL_QUERY_TABLE_KEYWORD) ||
          sourceName.equalsIgnoreCase("testDb.testTable");
    }

    @Override
    public TableDescriptor getTableDescriptor(String sourceName) {
      String[] sourceComponents = sourceName.split("\\.");

      TableDescriptor tableDescriptor = null;
      if (sourceComponents[0].equals(TEST_DB_SYSTEM)) {
        tableDescriptor = new TestTableDescriptor(TEST_TABLE_ID);;
      }

      return tableDescriptor;
    }
  }
}
