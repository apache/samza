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
package org.apache.samza.storage.kv;

import java.util.Arrays;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.MetricsConfig;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.context.JobContext;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;
import org.apache.samza.table.ReadWriteUpdateTable;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.*;


public class TestLocalTableWrite {

  public static final String TABLE_ID = "t1";

  private Timer putNs;
  private Timer putAllNs;
  private Timer deleteNs;
  private Timer deleteAllNs;
  private Timer flushNs;
  private Counter numPuts;
  private Counter numPutAlls;
  private Counter numDeletes;
  private Counter numDeleteAlls;
  private Counter numFlushes;
  private Timer putCallbackNs;
  private Timer deleteCallbackNs;

  private MetricsRegistry metricsRegistry;

  private KeyValueStore kvStore;

  @Before
  public void setUp() {

    putNs = new Timer("");
    putAllNs = new Timer("");
    deleteNs = new Timer("");
    deleteAllNs = new Timer("");
    flushNs = new Timer("");
    numPuts = new Counter("");
    numPutAlls = new Counter("");
    numDeletes = new Counter("");
    numDeleteAlls = new Counter("");
    numFlushes = new Counter("");
    putCallbackNs = new Timer("");
    deleteCallbackNs = new Timer("");

    metricsRegistry = mock(MetricsRegistry.class);
    String groupName = LocalTable.class.getSimpleName();
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-put-ns")).thenReturn(putNs);
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-putAll-ns")).thenReturn(putAllNs);
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-delete-ns")).thenReturn(deleteNs);
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-deleteAll-ns")).thenReturn(deleteAllNs);
    when(metricsRegistry.newCounter(groupName, TABLE_ID + "-num-puts")).thenReturn(numPuts);
    when(metricsRegistry.newCounter(groupName, TABLE_ID + "-num-putAlls")).thenReturn(numPutAlls);
    when(metricsRegistry.newCounter(groupName, TABLE_ID + "-num-deletes")).thenReturn(numDeletes);
    when(metricsRegistry.newCounter(groupName, TABLE_ID + "-num-deleteAlls")).thenReturn(numDeleteAlls);
    when(metricsRegistry.newCounter(groupName, TABLE_ID + "-num-flushes")).thenReturn(numFlushes);
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-put-callback-ns")).thenReturn(putCallbackNs);
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-delete-callback-ns")).thenReturn(deleteCallbackNs);
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-flush-ns")).thenReturn(flushNs);

    kvStore = mock(KeyValueStore.class);
  }

  @Test
  public void testPut() throws Exception {
    ReadWriteUpdateTable table = createTable(false);
    table.put("k1", "v1");
    table.putAsync("k2", "v2").get();
    table.putAsync("k3", null).get();
    verify(kvStore, times(2)).put(any(), any());
    verify(kvStore, times(1)).delete(any());
    Assert.assertEquals(2, numPuts.getCount());
    Assert.assertEquals(1, numDeletes.getCount());
    Assert.assertTrue(putNs.getSnapshot().getAverage() > 0);
    Assert.assertTrue(deleteNs.getSnapshot().getAverage() > 0);
    Assert.assertEquals(0, putAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, flushNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, numPutAlls.getCount());
    Assert.assertEquals(0, numDeleteAlls.getCount());
    Assert.assertEquals(0, numFlushes.getCount());
    Assert.assertEquals(0, putCallbackNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteCallbackNs.getSnapshot().getAverage(), 0.001);
  }

  @Test
  public void testPutAll() throws Exception {
    ReadWriteUpdateTable table = createTable(false);
    List<Entry> entries = Arrays.asList(new Entry("k1", "v1"), new Entry("k2", null));
    table.putAll(entries);
    table.putAllAsync(entries).get();
    verify(kvStore, times(2)).putAll(any());
    verify(kvStore, times(2)).deleteAll(any());
    Assert.assertEquals(2, numPutAlls.getCount());
    Assert.assertEquals(2, numDeleteAlls.getCount());
    Assert.assertTrue(putAllNs.getSnapshot().getAverage() > 0);
    Assert.assertTrue(deleteAllNs.getSnapshot().getAverage() > 0);
    Assert.assertEquals(0, putNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, flushNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, numPuts.getCount());
    Assert.assertEquals(0, numDeletes.getCount());
    Assert.assertEquals(0, numFlushes.getCount());
    Assert.assertEquals(0, putCallbackNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteCallbackNs.getSnapshot().getAverage(), 0.001);
  }

  @Test
  public void testDelete() throws Exception {
    ReadWriteUpdateTable table = createTable(false);
    table.delete("");
    table.deleteAsync("").get();
    verify(kvStore, times(2)).delete(any());
    Assert.assertEquals(2, numDeletes.getCount());
    Assert.assertTrue(deleteNs.getSnapshot().getAverage() > 0);
    Assert.assertEquals(0, putNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, putAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, flushNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, numPuts.getCount());
    Assert.assertEquals(0, numPutAlls.getCount());
    Assert.assertEquals(0, numDeleteAlls.getCount());
    Assert.assertEquals(0, numFlushes.getCount());
    Assert.assertEquals(0, putCallbackNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteCallbackNs.getSnapshot().getAverage(), 0.001);
  }

  @Test
  public void testDeleteAll() throws Exception {
    ReadWriteUpdateTable table = createTable(false);
    table.deleteAll(Collections.emptyList());
    table.deleteAllAsync(Collections.emptyList()).get();
    verify(kvStore, times(2)).deleteAll(any());
    Assert.assertEquals(2, numDeleteAlls.getCount());
    Assert.assertTrue(deleteAllNs.getSnapshot().getAverage() > 0);
    Assert.assertEquals(0, putNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, putAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, flushNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, numPuts.getCount());
    Assert.assertEquals(0, numPutAlls.getCount());
    Assert.assertEquals(0, numDeletes.getCount());
    Assert.assertEquals(0, numFlushes.getCount());
    Assert.assertEquals(0, putCallbackNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteCallbackNs.getSnapshot().getAverage(), 0.001);
  }

  @Test
  public void testFlush() {
    ReadWriteUpdateTable table = createTable(false);
    table.flush();
    table.flush();
    // Note: store.flush() is NOT called here
    verify(kvStore, times(0)).flush();
    Assert.assertEquals(2, numFlushes.getCount());
    Assert.assertTrue(flushNs.getSnapshot().getAverage() > 0);
    Assert.assertEquals(0, putNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, putAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, numPuts.getCount());
    Assert.assertEquals(0, numPutAlls.getCount());
    Assert.assertEquals(0, numDeletes.getCount());
    Assert.assertEquals(0, numDeleteAlls.getCount());
    Assert.assertEquals(0, putCallbackNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteCallbackNs.getSnapshot().getAverage(), 0.001);
  }

  @Test
  public void testTimerDisabled() throws Exception {
    ReadWriteUpdateTable table = createTable(true);
    table.put("", "");
    table.putAsync("", "").get();
    table.putAll(Collections.emptyList());
    table.putAllAsync(Collections.emptyList()).get();
    table.delete("");
    table.deleteAsync("").get();
    table.deleteAll(Collections.emptyList());
    table.deleteAllAsync(Collections.emptyList()).get();
    table.flush();
    Assert.assertEquals(1, numFlushes.getCount());
    Assert.assertEquals(2, numPuts.getCount());
    Assert.assertEquals(0, numPutAlls.getCount());
    Assert.assertEquals(2, numDeletes.getCount());
    Assert.assertEquals(2, numDeleteAlls.getCount());
    Assert.assertEquals(0, flushNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, putNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, putAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, putCallbackNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, deleteCallbackNs.getSnapshot().getAverage(), 0.001);
  }

  private LocalTable createTable(boolean isTimerDisabled) {
    Map<String, String> config = new HashMap<>();
    if (isTimerDisabled) {
      config.put(MetricsConfig.METRICS_TIMER_ENABLED, "false");
    }
    Context context = mock(Context.class);
    JobContext jobContext = mock(JobContext.class);
    when(context.getJobContext()).thenReturn(jobContext);
    when(jobContext.getConfig()).thenReturn(new MapConfig(config));
    ContainerContext containerContext = mock(ContainerContext.class);
    when(context.getContainerContext()).thenReturn(containerContext);
    when(containerContext.getContainerMetricsRegistry()).thenReturn(metricsRegistry);

    LocalTable table =  new LocalTable("t1", kvStore);
    table.init(context);

    return table;
  }
}
