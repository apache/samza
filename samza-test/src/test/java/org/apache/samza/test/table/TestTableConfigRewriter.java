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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaStorageConfig;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TableConfigRewriter;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.operators.TableDescriptor;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.LongSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.RocksDbKeyValueStorageEngineFactory;
import org.apache.samza.storage.kv.RocksDbTableDescriptor;
import org.apache.samza.storage.kv.RocksDbTableProviderFactory;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.table.TableDescriptorsFactory;
import org.apache.samza.table.remote.RemoteTableDescriptor;
import org.apache.samza.table.remote.RemoteTableProviderFactory;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.util.RateLimiter;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.*;


/**
 * Table config rewriter tests for both remote and local tables
 */
public class TestTableConfigRewriter {

  @Test
  public void testTableConfigRewriterWithNoConfiguredTaskClass() throws Exception {
    Map<String, String> configs = new HashMap<>();
    String tableRewriterName = "tableRewriter";
    Config resultConfig = new TableConfigRewriter().rewrite(tableRewriterName, new MapConfig(configs));
    Assert.assertTrue(resultConfig.size() == 0);
  }

  @Test
  public void testTableConfigRewriterWithNonTableDescriptorsFactoryTask() throws Exception {
    Map<String, String> configs = new HashMap<>();
    String tableRewriterName = "tableRewriter";
    configs.put(TaskConfig.TASK_CLASS(), SampleNonTableAwareTask.class.getName());
    Config resultConfig = new TableConfigRewriter().rewrite(tableRewriterName, new MapConfig(configs));
    Assert.assertTrue(resultConfig.size() == 1);
    JavaTableConfig tableConfig = new JavaTableConfig(resultConfig);
    Assert.assertTrue(tableConfig.getTableIds().size() == 0);
  }

  @Test
  public void testTableConfigRewriterWithTableDescriptorsFactoryTask() throws Exception {
    Map<String, String> configs = new HashMap<>();
    String tableRewriterName = "tableRewriter";
    configs.put(TaskConfig.TASK_CLASS(), SampleTableAwareTask.class.getName());
    Config resultConfig = new TableConfigRewriter().rewrite(tableRewriterName, new MapConfig(configs));
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

  public static class SampleNonTableAwareTask implements StreamTask {
    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
        throws Exception {
    }
  }

  public static class SampleTableAwareTask implements TableDescriptorsFactory, StreamTask {
    @Override
    public List<TableDescriptor> getTableDescriptors() {
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

    @Override
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator)
        throws Exception {
    }
  }
}
