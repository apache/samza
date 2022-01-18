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

public class TestLocalTableRead {

  public static final String TABLE_ID = "t1";

  private List<String> keys;
  private Map<String, String> values;

  private Timer getNs;
  private Timer getAllNs;
  private Counter numGets;
  private Counter numGetAlls;
  private Timer getCallbackNs;
  private Counter numMissedLookups;

  private MetricsRegistry metricsRegistry;

  private KeyValueStore kvStore;

  @Before
  public void setUp() {
    keys = Arrays.asList("k1", "k2", "k3");

    values = new HashMap<>();
    values.put("k1", "v1");
    values.put("k2", "v2");
    values.put("k3", null);

    kvStore = mock(KeyValueStore.class);
    when(kvStore.get("k1")).thenReturn("v1");
    when(kvStore.get("k2")).thenReturn("v2");
    when(kvStore.getAll(keys)).thenReturn(values);

    getNs = new Timer("");
    getAllNs = new Timer("");
    numGets = new Counter("");
    numGetAlls = new Counter("");
    getCallbackNs = new Timer("");
    numMissedLookups = new Counter("");

    metricsRegistry = mock(MetricsRegistry.class);
    String groupName = LocalTable.class.getSimpleName();
    when(metricsRegistry.newCounter(groupName, TABLE_ID + "-num-gets")).thenReturn(numGets);
    when(metricsRegistry.newCounter(groupName, TABLE_ID + "-num-getAlls")).thenReturn(numGetAlls);
    when(metricsRegistry.newCounter(groupName, TABLE_ID + "-num-missed-lookups")).thenReturn(numMissedLookups);
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-get-ns")).thenReturn(getNs);
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-getAll-ns")).thenReturn(getAllNs);
    when(metricsRegistry.newTimer(groupName, TABLE_ID + "-get-callback-ns")).thenReturn(getCallbackNs);
  }

  @Test
  public void testGet() throws Exception {
    ReadWriteUpdateTable table = createTable(false);
    Assert.assertEquals("v1", table.get("k1"));
    Assert.assertEquals("v2", table.getAsync("k2").get());
    Assert.assertNull(table.get("k3"));
    verify(kvStore, times(3)).get(any());
    Assert.assertEquals(3, numGets.getCount());
    Assert.assertEquals(1, numMissedLookups.getCount());
    Assert.assertTrue(getNs.getSnapshot().getAverage() > 0);
    Assert.assertEquals(0, numGetAlls.getCount());
    Assert.assertEquals(0, getAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, getCallbackNs.getSnapshot().getAverage(), 0.001);
  }

  @Test
  public void testGetAll() throws Exception {
    ReadWriteUpdateTable table = createTable(false);
    Assert.assertEquals(values, table.getAll(keys));
    Assert.assertEquals(values, table.getAllAsync(keys).get());
    verify(kvStore, times(2)).getAll(any());
    Assert.assertEquals(Collections.emptyMap(), table.getAll(Collections.emptyList()));
    Assert.assertEquals(2, numMissedLookups.getCount());
    Assert.assertEquals(3, numGetAlls.getCount());
    Assert.assertTrue(getAllNs.getSnapshot().getAverage() > 0);
    Assert.assertEquals(0, numGets.getCount());
    Assert.assertEquals(0, getNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, getCallbackNs.getSnapshot().getAverage(), 0.001);
  }

  @Test
  public void testTimerDisabled() throws Exception {
    ReadWriteUpdateTable table = createTable(true);
    table.get("");
    table.getAsync("").get();
    table.getAll(keys);
    table.getAllAsync(keys).get();
    Assert.assertEquals(2, numGets.getCount());
    Assert.assertEquals(4, numMissedLookups.getCount());
    Assert.assertEquals(2, numGetAlls.getCount());
    Assert.assertEquals(0, getNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, getAllNs.getSnapshot().getAverage(), 0.001);
    Assert.assertEquals(0, getCallbackNs.getSnapshot().getAverage(), 0.001);
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
