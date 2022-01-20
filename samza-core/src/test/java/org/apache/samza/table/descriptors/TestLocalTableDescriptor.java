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
package org.apache.samza.table.descriptors;

import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.StorageConfig;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.table.TableProvider;
import org.apache.samza.table.TableProviderFactory;
import org.junit.Test;

public class TestLocalTableDescriptor {

  private static final String JOB_NAME = "test-job";
  private static final String JOB_ID = "10";
  private static final String TABLE_ID = "t1";

  @Test
  public void testMinimal() {
    Config jobConfig = createJobConfig();
    Assert.assertEquals(2, jobConfig.size());
    Map<String, String> tableConfig = createTableDescriptor().toConfig(jobConfig);
    Assert.assertNotNull(tableConfig);
    Assert.assertEquals(1, tableConfig.size());
  }

  @Test
  public void testTableProviderFactoryConfig() {
    Map<String, String> tableConfig = createTableDescriptor()
        .toConfig(createJobConfig());
    Assert.assertEquals(1, tableConfig.size());
    Assert.assertEquals(MockTableProviderFactory.class.getName(),
        tableConfig.get(String.format(JavaTableConfig.TABLE_PROVIDER_FACTORY, TABLE_ID)));
  }

  @Test
  public void testStoreConfig() {
    Map<String, String> tableConfig = createTableDescriptor()
        .toConfig(createJobConfig());
    Assert.assertEquals(1, tableConfig.size());
  }

  @Test
  public void testChangelogDisabled() {
    Map<String, String> tableConfig = createTableDescriptor()
        .toConfig(createJobConfig());
    Assert.assertEquals(1, tableConfig.size());
    Assert.assertFalse(tableConfig.containsKey(String.format(StorageConfig.CHANGELOG_STREAM, TABLE_ID)));
  }

  @Test
  public void testChangelogEnabled() {
    Map<String, String> tableConfig = createTableDescriptor()
        .withChangelogEnabled()
        .toConfig(createJobConfig());
    Assert.assertEquals(2, tableConfig.size());
    Assert.assertEquals("test-job-10-table-t1", String.format(
        tableConfig.get(String.format(StorageConfig.CHANGELOG_STREAM, TABLE_ID))));
  }

  @Test
  public void testChangelogEnabledWithCustomParameters() {
    Map<String, String> tableConfig = createTableDescriptor()
        .withChangelogStream("my-stream")
        .withChangelogReplicationFactor(100)
        .toConfig(createJobConfig());
    Assert.assertEquals(3, tableConfig.size());
    Assert.assertEquals("my-stream", String.format(
        tableConfig.get(String.format(StorageConfig.CHANGELOG_STREAM, TABLE_ID))));
    Assert.assertEquals("100", String.format(
        tableConfig.get(String.format(StorageConfig.CHANGELOG_REPLICATION_FACTOR, TABLE_ID))));
  }

  @Test(expected = NullPointerException.class)
  public void testChangelogWithoutJobName() {
    Map<String, String> jobConfig = new HashMap<>();
    jobConfig.put("job.id", JOB_ID);
    createTableDescriptor()
        .withChangelogEnabled()
        .toConfig(new MapConfig(jobConfig));
  }

  @Test(expected = NullPointerException.class)
  public void testChangelogWithoutJobId() {
    Map<String, String> jobConfig = new HashMap<>();
    jobConfig.put("job.name", JOB_NAME);
    createTableDescriptor()
        .withChangelogEnabled()
        .toConfig(new MapConfig(jobConfig));
  }

  private Config createJobConfig() {
    Map<String, String> jobConfig = new HashMap<>();
    jobConfig.put("job.name", JOB_NAME);
    jobConfig.put("job.id", JOB_ID);
    return new MapConfig(jobConfig);
  }

  private LocalTableDescriptor createTableDescriptor() {
    return new MockLocalTableDescriptor(TABLE_ID, new KVSerde(new StringSerde(), new IntegerSerde()));
  }

  public static class MockLocalTableDescriptor<K, V> extends LocalTableDescriptor<K, V, MockLocalTableDescriptor<K, V>> {

    public MockLocalTableDescriptor(String tableId, KVSerde<K, V> serde) {
      super(tableId, serde);
    }

    public String getProviderFactoryClassName() {
      return MockTableProviderFactory.class.getName();
    }
  }

  public static class MockTableProvider implements TableProvider {
    @Override
    public void init(Context context) {
    }

    @Override
    public ReadWriteUpdateTable getTable() {
      throw new SamzaException("Not implemented");
    }

    @Override
    public void close() {
    }
  }

  public static class MockTableProviderFactory implements TableProviderFactory {
    @Override
    public TableProvider getTableProvider(String tableId) {
      return new MockTableProvider();
    }
  }

}
