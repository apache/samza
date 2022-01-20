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

import junit.framework.Assert;
import org.apache.samza.SamzaException;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.SerializerConfig;
import org.apache.samza.context.MockContext;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerializableSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.StorageEngine;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestTableManager {

  private static final String TABLE_ID = "t1";

  public static class DummyTableProviderFactory implements TableProviderFactory {

    static ReadWriteUpdateTable table;
    static TableProvider tableProvider;

    @Override
    public TableProvider getTableProvider(String tableId) {
      table = mock(ReadWriteUpdateTable.class);
      tableProvider = mock(TableProvider.class);
      when(tableProvider.getTable()).thenReturn(table);
      return tableProvider;
    }
  }

  @Test
  public void testInitByConfig() {
    Map<String, String> map = new HashMap<>();
    map.put(String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, TABLE_ID), DummyTableProviderFactory.class.getName());
    map.put(String.format("tables.%s.some.config", TABLE_ID), "xyz");
    addKeySerde(map);
    addValueSerde(map);
    doTestInit(map);
  }

  @Test(expected = Exception.class)
  public void testInitFailsWithoutProviderFactory() {
    Map<String, String> map = new HashMap<>();
    addKeySerde(map);
    addValueSerde(map);
    doTestInit(map);
  }

  @Test(expected = IllegalStateException.class)
  public void testInitFailsWithoutInitializingLocalStores() {
    TableManager tableManager = new TableManager(new MapConfig(new HashMap<>()));
    tableManager.getTable("dummy");
  }

  private void doTestInit(Map<String, String> map) {
    Map<String, StorageEngine> storageEngines = new HashMap<>();
    storageEngines.put(TABLE_ID, mock(StorageEngine.class));
    TableManager tableManager = new TableManager(new MapConfig(map));
    tableManager.init(new MockContext());

    for (int i = 0; i < 2; i++) {
      Table table = tableManager.getTable(TABLE_ID);
      verify(DummyTableProviderFactory.tableProvider, times(1)).init(anyObject());
      verify(DummyTableProviderFactory.tableProvider, times(1)).getTable();
      Assert.assertEquals(DummyTableProviderFactory.table, table);
    }

    Map<String, TableManager.TableCtx> ctxMap = getFieldValue(tableManager, "tableContexts");
    TableManager.TableCtx ctx = ctxMap.get(TABLE_ID);
    Assert.assertEquals(TABLE_ID, ctxMap.keySet().iterator().next());

    TableProvider tableProvider = getFieldValue(ctx, "tableProvider");
    Assert.assertNotNull(tableProvider);
  }

  private void addKeySerde(Map<String, String> map) {
    String serdeId = "key-serde";
    map.put(String.format(SerializerConfig.SERDE_SERIALIZED_INSTANCE, serdeId),
        serializeSerde(new IntegerSerde()));
    map.put(String.format(JavaTableConfig.STORE_KEY_SERDE, TABLE_ID), serdeId);
  }

  private void addValueSerde(Map<String, String> map) {
    String serdeId = "value-serde";
    map.put(String.format(SerializerConfig.SERDE_SERIALIZED_INSTANCE, serdeId),
            serializeSerde(new StringSerde("UTF-8")));
    map.put(String.format(JavaTableConfig.STORE_MSG_SERDE, TABLE_ID), serdeId);
  }

  private String serializeSerde(Serde serde) {
    return Base64.getEncoder().encodeToString(new SerializableSerde().toBytes(serde));
  }

  private <T> T getFieldValue(Object object, String fieldName) {
    Field field = null;
    try {
      field = object.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return (T) field.get(object);
    } catch (NoSuchFieldException | IllegalAccessException ex) {
      throw new SamzaException(ex);
    } finally {
      if (field != null) {
        field.setAccessible(false);
      }
    }
  }

}
