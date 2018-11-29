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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.samza.SamzaException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.container.SamzaContainerContext;
import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.operators.KV;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.table.Table;
import org.apache.samza.table.remote.TableReadFunction;
import org.apache.samza.table.remote.TableWriteFunction;
import org.apache.samza.table.remote.RemoteReadableTable;
import org.apache.samza.table.remote.RemoteTableDescriptor;
import org.apache.samza.table.remote.RemoteReadWriteTable;
import org.apache.samza.task.TaskContext;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.util.Base64Serializer;
import org.apache.samza.util.RateLimiter;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;


public class TestRemoteTable extends AbstractIntegrationTestHarness {
  private TableReadFunction<Integer, TestTableData.Profile> getInMemoryReader(TestTableData.Profile[] profiles) {
    final Map<Integer, TestTableData.Profile> profileMap = Arrays.stream(profiles)
        .collect(Collectors.toMap(p -> p.getMemberId(), Function.identity()));
    TableReadFunction<Integer, TestTableData.Profile> reader =
        (TableReadFunction<Integer, TestTableData.Profile>) key -> profileMap.getOrDefault(key, null);
    return reader;
  }

  static List<TestTableData.EnrichedPageView> writtenRecords = new LinkedList<>();

  static class InMemoryWriteFunction implements TableWriteFunction<Integer, TestTableData.EnrichedPageView> {
    private transient List<TestTableData.EnrichedPageView> records;

    // Verify serializable functionality
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
      in.defaultReadObject();

      // Write to the global list for verification
      records = writtenRecords;
    }

    @Override
    public void put(Integer key, TestTableData.EnrichedPageView record) {
      records.add(record);
    }

    @Override
    public void delete(Integer key) {
      records.remove(key);
    }

    @Override
    public void deleteAll(Collection<Integer> keys) {
      records.removeAll(keys);
    }
  }

  @Test
  public void testStreamTableJoinRemoteTable() throws Exception {
    List<TestTableData.PageView> received = new LinkedList<>();
    final InMemoryWriteFunction writer = new InMemoryWriteFunction();

    int count = 10;
    TestTableData.PageView[] pageViews = TestTableData.generatePageViews(count);
    TestTableData.Profile[] profiles = TestTableData.generateProfiles(count);

    int partitionCount = 4;
    Map<String, String> configs = TestLocalTable.getBaseJobConfig(bootstrapUrl(), zkConnect());

    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageViews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));

    final RateLimiter readRateLimiter = mock(RateLimiter.class);
    final RateLimiter writeRateLimiter = mock(RateLimiter.class);
    final LocalApplicationRunner runner = new LocalApplicationRunner(new MapConfig(configs));
    final StreamApplication app = (streamGraph, cfg) -> {
      RemoteTableDescriptor<Integer, TestTableData.Profile> inputTableDesc = new RemoteTableDescriptor<>("profile-table-1");
      inputTableDesc
          .withReadFunction(getInMemoryReader(profiles))
          .withRateLimiter(readRateLimiter, null, null);

      RemoteTableDescriptor<Integer, TestTableData.EnrichedPageView> outputTableDesc = new RemoteTableDescriptor<>("enriched-page-view-table-1");
      outputTableDesc
          .withReadFunction(key -> null) // dummy reader
          .withWriteFunction(writer)
          .withRateLimiter(writeRateLimiter, null, null);

      Table<KV<Integer, TestTableData.Profile>> inputTable = streamGraph.getTable(inputTableDesc);
      Table<KV<Integer, TestTableData.EnrichedPageView>> outputTable = streamGraph.getTable(outputTableDesc);

      streamGraph.getInputStream("PageView", new NoOpSerde<TestTableData.PageView>())
          .map(pv -> {
              received.add(pv);
              return new KV<Integer, TestTableData.PageView>(pv.getMemberId(), pv);
            })
          .join(inputTable, new TestLocalTable.PageViewToProfileJoinFunction())
          .map(m -> new KV(m.getMemberId(), m))
          .sendTo(outputTable);
    };

    runner.run(app);
    runner.waitForFinish();

    int numExpected = count * partitionCount;
    Assert.assertEquals(numExpected, received.size());
    Assert.assertEquals(numExpected, writtenRecords.size());
    Assert.assertTrue(writtenRecords.get(0) instanceof TestTableData.EnrichedPageView);
  }

  private SamzaContainerContext createMockContainerContext() {
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    doReturn(mock(Counter.class)).when(metricsRegistry).newCounter(anyString(), anyString());
    doReturn(mock(Timer.class)).when(metricsRegistry).newTimer(anyString(), anyString());
    return new SamzaContainerContext("1", new MapConfig(), Arrays.asList(new TaskName("task-1")),
        metricsRegistry);
  }

  @Test(expected = SamzaException.class)
  public void testCatchReaderException() {
    TableReadFunction<String, ?> reader = mock(TableReadFunction.class);
    doThrow(new RuntimeException("Expected test exception")).when(reader).get(anyString());
    RemoteReadableTable<String, ?> table = new RemoteReadableTable<>("table1", reader, null, null);
    table.init(createMockContainerContext(), mock(TaskContext.class));
    table.get("abc");
  }

  @Test(expected = SamzaException.class)
  public void testCatchWriterException() {
    TableReadFunction<String, String> reader = mock(TableReadFunction.class);
    TableWriteFunction<String, String> writer = mock(TableWriteFunction.class);
    doThrow(new RuntimeException("Expected test exception")).when(writer).put(anyString(), any());
    RemoteReadWriteTable<String, String> table = new RemoteReadWriteTable<>("table1", reader, writer, null, null, null);
    table.init(createMockContainerContext(), mock(TaskContext.class));
    table.put("abc", "efg");
  }

  @Test
  public void testMetrics() {
    List<String> keys = Arrays.asList("k1", "k2");

    Map<String, String> values = new HashMap<>();
    values.put("k1", "v1");
    values.put("k2", null);

    TableReadFunction<String, ?> reader = mock(TableReadFunction.class);
    doReturn("v1").when(reader).get("k1");
    doReturn("v2").when(reader).get("k2");
    doReturn(values).when(reader).getAll(keys);

    TableWriteFunction<String, ?> writer = mock(TableWriteFunction.class);

    Map<String, String> config = new HashMap<>();
    config.put(MetricsConfig.METRICS_TIMER_ENABLED(), "false");
    MetricsRegistry metricsRegistry = mock(MetricsRegistry.class);
    // From remote readable table
    Timer getNs = mock(Timer.class);
    Timer getThrottleNs = mock(Timer.class);
    Counter numGets = mock(Counter.class);
    doReturn(getNs).when(metricsRegistry).newTimer(any(), eq("t1-get-ns"));
    doReturn(getThrottleNs).when(metricsRegistry).newTimer(any(), eq("t1-get-throttle-ns"));
    doReturn(numGets).when(metricsRegistry).newCounter(any(), eq("t1-num-gets"));
    // From remote read/write table
    Timer putNs = mock(Timer.class);
    Timer deleteNs = mock(Timer.class);
    Timer flushNs = mock(Timer.class);
    Timer putThrottleNs = mock(Timer.class);
    Counter numPuts = mock(Counter.class);
    Counter numDeletes = mock(Counter.class);
    Counter numFlushes = mock(Counter.class);
    doReturn(putNs).when(metricsRegistry).newTimer(any(), eq("t1-put-ns"));
    doReturn(deleteNs).when(metricsRegistry).newTimer(any(), eq("t1-delete-ns"));
    doReturn(flushNs).when(metricsRegistry).newTimer(any(), eq("t1-flush-ns"));
    doReturn(putThrottleNs).when(metricsRegistry).newTimer(any(), eq("t1-put-throttle-ns"));
    doReturn(numPuts).when(metricsRegistry).newCounter(any(), eq("t1-num-puts"));
    doReturn(numDeletes).when(metricsRegistry).newCounter(any(), eq("t1-num-deletes"));
    doReturn(numFlushes).when(metricsRegistry).newCounter(any(), eq("t1-num-flushes"));

    // With timers disabled
    //
    SamzaContainerContext containerContext1 = new SamzaContainerContext("1", new MapConfig(config),
        Arrays.asList(new TaskName("task-1")), metricsRegistry);

    RemoteReadableTable<String, ?> roTable1 = new RemoteReadableTable<>(
        "t1", reader, null, null);
    roTable1.init(containerContext1, mock(TaskContext.class));
    verify(metricsRegistry, atLeastOnce()).newCounter(any(), anyString());
    verify(metricsRegistry, times(0)).newTimer(any(), anyString());

    RemoteReadWriteTable<String, String> rwTable1 = new RemoteReadWriteTable<>(
        "t1", reader, writer, null, null, null);
    rwTable1.init(containerContext1, mock(TaskContext.class));
    verify(metricsRegistry, atLeastOnce()).newCounter(any(), anyString());
    verify(metricsRegistry, times(0)).newTimer(any(), anyString());

    // With timers enabled
    //
    SamzaContainerContext containerContext2 = new SamzaContainerContext("1", new MapConfig(),
        Arrays.asList(new TaskName("task-1")), metricsRegistry);

    RemoteReadableTable<String, ?> roTable2 = new RemoteReadableTable<>(
        "t1", reader, null, null);
    roTable2.init(containerContext2, mock(TaskContext.class));
    Assert.assertEquals("v1", roTable2.get("k1"));
    Assert.assertEquals("v2", roTable2.get("k2"));
    Assert.assertEquals(values, roTable2.getAll(keys));
    verify(numGets, times(2)).inc();
    verify(getNs, times(2)).update(anyLong());
    verify(getThrottleNs, times(0)).update(anyLong());

    RemoteReadWriteTable<String, String> rwTable2 = new RemoteReadWriteTable<>(
        "t1", reader, writer, null, null, null);
    rwTable2.init(containerContext2, mock(TaskContext.class));
    rwTable2.put("k1", "v1");
    rwTable2.put("k2", "v2");
    Assert.assertEquals("v1", rwTable2.get("k1"));
    Assert.assertEquals("v2", rwTable2.get("k2"));
    rwTable2.delete("k1");
    rwTable2.delete("k2");
    rwTable2.flush();
    rwTable2.flush();
    verify(numGets, times(4)).inc();
    verify(getNs, times(4)).update(anyLong());
    verify(getThrottleNs, times(0)).update(anyLong());
    verify(numPuts, times(2)).inc();
    verify(putNs, times(2)).update(anyLong());
    verify(putThrottleNs, times(0)).update(anyLong());
    verify(numDeletes, times(2)).inc();
    verify(deleteNs, times(2)).update(anyLong());
    verify(numFlushes, times(2)).inc();
    verify(flushNs, times(2)).update(anyLong());
  }

}
