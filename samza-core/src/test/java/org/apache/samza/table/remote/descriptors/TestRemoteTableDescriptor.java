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

package org.apache.samza.table.remote.descriptors;

import com.google.common.collect.ImmutableMap;

import org.apache.samza.config.Config;
import org.apache.samza.config.JavaTableConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.container.TaskName;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.context.JobContext;
import org.apache.samza.context.TaskContextImpl;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.job.model.TaskModel;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.AsyncReadWriteUpdateTable;
import org.apache.samza.table.Table;
import org.apache.samza.table.descriptors.RemoteTableDescriptor;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.table.ratelimit.AsyncRateLimitedTable;
import org.apache.samza.table.remote.AsyncRemoteTable;
import org.apache.samza.table.remote.RemoteTable;
import org.apache.samza.table.remote.RemoteTableProvider;
import org.apache.samza.table.remote.TablePart;
import org.apache.samza.table.remote.TableRateLimiter;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.retry.AsyncRetriableTable;
import org.apache.samza.table.retry.TableRetryPolicy;
import org.apache.samza.testUtils.TestUtils;
import org.apache.samza.util.EmbeddedTaggedRateLimiter;
import org.apache.samza.util.RateLimiter;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import static org.apache.samza.table.remote.TableRateLimiter.CreditFunction;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;


public class TestRemoteTableDescriptor {

  private void doTestSerialize(RateLimiter rateLimiter, CreditFunction readCredFn, CreditFunction writeCredFn) {
    String tableId = "1";
    RemoteTableDescriptor desc = new RemoteTableDescriptor(tableId)
        .withReadFunction(createMockTableReadFunction())
        .withWriteFunction(createMockTableWriteFunction());
    if (rateLimiter != null) {
      desc.withRateLimiter(rateLimiter, readCredFn, writeCredFn);
      if (readCredFn == null) {
        desc.withReadRateLimiterDisabled();
      }
      if (writeCredFn == null) {
        desc.withWriteRateLimiterDisabled();
      }
    } else {
      desc.withReadRateLimit(100);
      desc.withWriteRateLimit(200);
    }
    Map<String, String> tableConfig = desc.toConfig(new MapConfig());
    assertExists(RemoteTableDescriptor.RATE_LIMITER, tableId, tableConfig);
    Assert.assertEquals(readCredFn != null,
        tableConfig.containsKey(JavaTableConfig.buildKey(tableId, RemoteTableDescriptor.READ_CREDIT_FN)));
    Assert.assertEquals(writeCredFn != null,
        tableConfig.containsKey(JavaTableConfig.buildKey(tableId, RemoteTableDescriptor.WRITE_CREDIT_FN)));
  }

  @Test
  public void testValidateOnlyReadOrWriteFn() {
    // Only read defined
    String tableId = "1";
    RemoteTableDescriptor desc = new RemoteTableDescriptor(tableId)
        .withReadFunction(createMockTableReadFunction())
        .withReadRateLimiterDisabled();
    Map<String, String> tableConfig = desc.toConfig(new MapConfig());
    Assert.assertNotNull(tableConfig);

    // Only write defined
    String tableId2 = "2";
    RemoteTableDescriptor desc2 = new RemoteTableDescriptor(tableId2)
        .withWriteFunction(createMockTableWriteFunction())
        .withWriteRateLimiterDisabled();
    tableConfig = desc2.toConfig(new MapConfig());
    Assert.assertNotNull(tableConfig);

    // Neither read or write defined (Failure case)
    String tableId3 = "3";
    RemoteTableDescriptor desc3 = new RemoteTableDescriptor(tableId3);
    try {
      desc3.toConfig(new MapConfig());
      Assert.fail("Should not allow neither readFn or writeFn defined");
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
      Assert.assertTrue(e.getMessage().contains("Must have one of TableReadFunction or TableWriteFunction"));
    }
  }


  @Test
  public void testSerializeSimple() {
    doTestSerialize(null, null, null);
  }

  @Test
  public void testSerializeWithLimiter() {
    doTestSerialize(createMockRateLimiter(), new CountingCreditFunction(), new CountingCreditFunction());
  }

  @Test
  public void testSerializeWithLimiterAndReadCredFn() {
    doTestSerialize(createMockRateLimiter(), (k, v, args) -> 1, null);
  }

  @Test
  public void testSerializeWithLimiterAndWriteCredFn() {
    doTestSerialize(createMockRateLimiter(), null, (k, v, args) -> 1);
  }

  @Test
  public void testSerializeNullWriteFunction() {
    String tableId = "1";
    RemoteTableDescriptor desc = new RemoteTableDescriptor(tableId)
        .withReadFunction(createMockTableReadFunction())
        .withRateLimiterDisabled();
    Map<String, String> tableConfig = desc.toConfig(new MapConfig());
    assertExists(RemoteTableDescriptor.READ_FN, tableId, tableConfig);
    assertEquals(null, RemoteTableDescriptor.WRITE_FN, tableId, tableConfig);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSerializeNullReadFunction() {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    Map<String, String> tableConfig = desc.toConfig(new MapConfig());
    Assert.assertTrue(tableConfig.containsKey(RemoteTableDescriptor.READ_FN));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSpecifyBothRateAndRateLimiter() {
    RemoteTableDescriptor desc = new RemoteTableDescriptor("1");
    desc.withReadFunction(createMockTableReadFunction());
    desc.withReadRateLimit(100);
    desc.withRateLimiter(createMockRateLimiter(), null, null);
    desc.toConfig(new MapConfig());
  }

  @Test
  public void testTablePartToConfigDefault() {
    TableReadFunction readFn = createMockTableReadFunction();
    when(readFn.toConfig(any(), any())).thenReturn(createConfigPair(1));
    Map<String, String> tableConfig = new RemoteTableDescriptor("1")
        .withReadFunction(readFn)
        .withReadRateLimit(100)
        .toConfig(new MapConfig());
    verify(readFn, times(1)).toConfig(any(), any());
    Assert.assertEquals("v1", tableConfig.get("tables.1.io.read.func.k1"));
  }

  @Test
  public void testTablePartToConfig() {

    int key = 0;

    TableReadFunction readFn = createMockTableReadFunction();
    when(readFn.toConfig(any(), any())).thenReturn(createConfigPair(key));

    TableWriteFunction writeFn = createMockTableWriteFunction();
    when(writeFn.toConfig(any(), any())).thenReturn(createConfigPair(key));

    RateLimiter rateLimiter = createMockRateLimiter();
    when(((TablePart) rateLimiter).toConfig(any(), any())).thenReturn(createConfigPair(key));

    CreditFunction readCredFn = createMockCreditFunction();
    when(readCredFn.toConfig(any(), any())).thenReturn(createConfigPair(key));

    CreditFunction writeCredFn = createMockCreditFunction();
    when(writeCredFn.toConfig(any(), any())).thenReturn(createConfigPair(key));

    TableRetryPolicy readRetryPolicy = createMockTableRetryPolicy();
    when(readRetryPolicy.toConfig(any(), any())).thenReturn(createConfigPair(key));

    TableRetryPolicy writeRetryPolicy = createMockTableRetryPolicy();
    when(writeRetryPolicy.toConfig(any(), any())).thenReturn(createConfigPair(key));

    Map<String, String> tableConfig = new RemoteTableDescriptor("1").withReadFunction(readFn)
        .withWriteFunction(writeFn)
        .withRateLimiter(rateLimiter, readCredFn, writeCredFn)
        .withReadRetryPolicy(readRetryPolicy)
        .withWriteRetryPolicy(writeRetryPolicy)
        .toConfig(new MapConfig());

    verify(readFn, times(1)).toConfig(any(), any());
    verify(writeFn, times(1)).toConfig(any(), any());
    verify((TablePart) rateLimiter, times(1)).toConfig(any(), any());
    verify(readCredFn, times(1)).toConfig(any(), any());
    verify(writeCredFn, times(1)).toConfig(any(), any());
    verify(readRetryPolicy, times(1)).toConfig(any(), any());
    verify(writeRetryPolicy, times(1)).toConfig(any(), any());

    Assert.assertEquals(tableConfig.get("tables.1.io.read.func.k0"), "v0");
    Assert.assertEquals(tableConfig.get("tables.1.io.write.func.k0"), "v0");
    Assert.assertEquals(tableConfig.get("tables.1.io.ratelimiter.k0"), "v0");
    Assert.assertEquals(tableConfig.get("tables.1.io.read.credit.func.k0"), "v0");
    Assert.assertEquals(tableConfig.get("tables.1.io.write.credit.func.k0"), "v0");
    Assert.assertEquals(tableConfig.get("tables.1.io.read.retry.policy.k0"), "v0");
    Assert.assertEquals(tableConfig.get("tables.1.io.write.retry.policy.k0"), "v0");
  }

  @Test
  public void testTableRetryPolicyToConfig() {
    Map<String, String> tableConfig = new RemoteTableDescriptor("1").withReadFunction(createMockTableReadFunction())
        .withReadRetryPolicy(new TableRetryPolicy())
        .withRateLimiterDisabled()
        .toConfig(new MapConfig());
    Assert.assertEquals(tableConfig.get("tables.1.io.read.retry.policy.TableRetryPolicy"),
        "{\"exponentialFactor\":0.0,\"backoffType\":\"NONE\",\"retryPredicate\":{}}");
  }

  @Test
  public void testReadWriteRateLimitToConfig() {
    Map<String, String> tableConfig = new RemoteTableDescriptor("1").withReadFunction(createMockTableReadFunction())
        .withReadRetryPolicy(new TableRetryPolicy())
        .withWriteRateLimit(1000)
        .withReadRateLimit(2000)
        .toConfig(new MapConfig());
    Assert.assertEquals(String.valueOf(2000), tableConfig.get("tables.1.io.read.credits"));
    Assert.assertEquals(String.valueOf(1000), tableConfig.get("tables.1.io.write.credits"));
  }

  private Context createMockContext(TableDescriptor tableDescriptor) {
    Context context = mock(Context.class);

    ContainerContext containerContext = mock(ContainerContext.class);
    when(context.getContainerContext()).thenReturn(containerContext);

    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    when(metricsRegistry.newTimer(anyString(), anyString())).thenReturn(mock(Timer.class));
    when(metricsRegistry.newCounter(anyString(), anyString())).thenReturn(mock(Counter.class));
    when(containerContext.getContainerMetricsRegistry()).thenReturn(metricsRegistry);

    TaskContextImpl taskContext = mock(TaskContextImpl.class);
    when(context.getTaskContext()).thenReturn(taskContext);

    TaskName taskName = new TaskName("MyTask");
    TaskModel taskModel = mock(TaskModel.class);
    when(taskModel.getTaskName()).thenReturn(taskName);
    when(context.getTaskContext().getTaskModel()).thenReturn(taskModel);

    ContainerModel containerModel = mock(ContainerModel.class);
    when(containerModel.getTasks()).thenReturn(ImmutableMap.of(taskName, taskModel));
    when(containerContext.getContainerModel()).thenReturn(containerModel);

    String containerId = "container-1";
    JobModel jobModel = mock(JobModel.class);
    when(taskContext.getJobModel()).thenReturn(jobModel);
    when(jobModel.getContainers()).thenReturn(ImmutableMap.of(containerId, containerModel));

    JobContext jobContext = mock(JobContext.class);
    Config jobConfig = new MapConfig(tableDescriptor.toConfig(new MapConfig()));
    when(jobContext.getConfig()).thenReturn(jobConfig);
    when(context.getJobContext()).thenReturn(jobContext);

    return context;
  }

  static class CountingCreditFunction<K, V> implements CreditFunction<K, V> {
    int numCalls = 0;

    @Override
    public int getCredits(K key, V value, Object ... args) {
      numCalls++;
      return 1;
    }
  }

  private void doTestDeserializeReadFunctionAndLimiter(boolean rateOnly, boolean rlGets, boolean rlPuts) {
    int numRateLimitOps = (rlGets ? 1 : 0) + (rlPuts ? 1 : 0);
    RemoteTableDescriptor<String, String, String> desc = new RemoteTableDescriptor("1")
        .withReadFunction(createMockTableReadFunction())
        .withReadRetryPolicy(new TableRetryPolicy().withRetryPredicate((ex) -> false))
        .withWriteFunction(createMockTableWriteFunction())
        .withAsyncCallbackExecutorPoolSize(10);

    if (rateOnly) {
      if (rlGets) {
        desc.withReadRateLimit(1000);
      } else {
        desc.withReadRateLimiterDisabled();
      }
      if (rlPuts) {
        desc.withWriteRateLimit(2000);
      } else {
        desc.withWriteRateLimiterDisabled();
      }
    } else {
      if (numRateLimitOps > 0) {
        Map<String, Integer> tagCredits = new HashMap<>();
        if (rlGets) {
          tagCredits.put(RemoteTableDescriptor.RL_READ_TAG, 1000);
        } else {
          desc.withReadRateLimiterDisabled();
        }
        if (rlPuts) {
          tagCredits.put(RemoteTableDescriptor.RL_WRITE_TAG, 2000);
        } else {
          desc.withWriteRateLimiterDisabled();
        }

        // Spy the rate limiter to verify call count
        RateLimiter rateLimiter = spy(new EmbeddedTaggedRateLimiter(tagCredits));
        desc.withRateLimiter(rateLimiter, new CountingCreditFunction(), new CountingCreditFunction());
      } else {
        desc.withRateLimiterDisabled();
      }
    }

    RemoteTableProvider provider = new RemoteTableProvider(desc.getTableId());
    provider.init(createMockContext(desc));
    Table table = provider.getTable();
    Assert.assertTrue(table instanceof RemoteTable);
    RemoteTable rwTable = (RemoteTable) table;

    AsyncReadWriteUpdateTable delegate = TestUtils.getFieldValue(rwTable, "asyncTable");
    Assert.assertTrue(delegate instanceof AsyncRetriableTable);
    if (rlGets || rlPuts) {
      delegate = TestUtils.getFieldValue(delegate, "table");
      Assert.assertTrue(delegate instanceof AsyncRateLimitedTable);
    }
    delegate = TestUtils.getFieldValue(delegate, "table");
    Assert.assertTrue(delegate instanceof AsyncRemoteTable);

    if (numRateLimitOps > 0) {
      TableRateLimiter readRateLimiter = TestUtils.getFieldValue(rwTable, "readRateLimiter");
      TableRateLimiter writeRateLimiter = TestUtils.getFieldValue(rwTable, "writeRateLimiter");
      Assert.assertTrue(!rlGets || readRateLimiter != null);
      Assert.assertTrue(!rlPuts || writeRateLimiter != null);
    }

    ThreadPoolExecutor callbackExecutor = TestUtils.getFieldValue(rwTable, "callbackExecutor");
    Assert.assertEquals(10, callbackExecutor.getCorePoolSize());
  }

  @Test
  public void testDeserializeReadFunctionNoRateLimit() {
    doTestDeserializeReadFunctionAndLimiter(false, false, false);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterWrite() {
    doTestDeserializeReadFunctionAndLimiter(false, false, true);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterRead() {
    doTestDeserializeReadFunctionAndLimiter(false, true, false);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterReadWrite() {
    doTestDeserializeReadFunctionAndLimiter(false, true, true);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterRateOnlyWrite() {
    doTestDeserializeReadFunctionAndLimiter(true, false, true);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterRateOnlyRead() {
    doTestDeserializeReadFunctionAndLimiter(true, true, false);
  }

  @Test
  public void testDeserializeReadFunctionAndLimiterRateOnlyReadWrite() {
    doTestDeserializeReadFunctionAndLimiter(true, true, true);
  }

  private RateLimiter createMockRateLimiter() {
    return mock(RateLimiter.class, withSettings().serializable().extraInterfaces(TablePart.class));
  }

  private CreditFunction createMockCreditFunction() {
    return mock(CreditFunction.class, withSettings().serializable());
  }

  private TableReadFunction createMockTableReadFunction() {
    return mock(TableReadFunction.class, withSettings().serializable());
  }

  private TableWriteFunction createMockTableWriteFunction() {
    return mock(TableWriteFunction.class, withSettings().serializable());
  }

  private TableRetryPolicy createMockTableRetryPolicy() {
    return mock(TableRetryPolicy.class, withSettings().serializable());
  }

  private Map<String, String> createConfigPair(int n) {
    Map<String, String> config = new HashMap<>();
    config.put("k" + n, "v" + n);
    return config;
  }

  private void assertExists(String key, String tableId, Map<String, String> config) {
    String realKey = JavaTableConfig.buildKey(tableId, key);
    Assert.assertTrue(config.containsKey(realKey));
  }

  private void assertEquals(String expectedValue, String key, String tableId, Map<String, String> config) {
    String realKey = JavaTableConfig.buildKey(tableId, key);
    Assert.assertEquals(expectedValue, config.get(realKey));
  }
}
