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

package org.apache.samza.storage.kv.descriptors;

import junit.framework.Assert;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.context.Context;
import org.apache.samza.context.TaskContext;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.storage.kv.LocalTableProvider;
import org.apache.samza.table.TableProvider;
import org.apache.samza.util.NoOpMetricsRegistry;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


public class TestLocalTableProvider {

  @Test
  public void testInit() {
    Context context = mock(Context.class);
    TaskContext taskContext = mock(TaskContext.class);
    when(context.getTaskContext()).thenReturn(taskContext);
    when(taskContext.getStore(any())).thenReturn(mock(KeyValueStore.class));
    when(taskContext.getTaskMetricsRegistry()).thenReturn(new NoOpMetricsRegistry());

    TableProvider tableProvider = createTableProvider("t1");
    tableProvider.init(context);
    Assert.assertNotNull(tableProvider.getTable());
  }

  @Test(expected = SamzaException.class)
  public void testInitFail() {
    TableProvider tableProvider = createTableProvider("t1");
    Assert.assertNotNull(tableProvider.getTable());
  }

  private TableProvider createTableProvider(String tableId) {
    return new LocalTableProvider(tableId, new MapConfig()) {
    };
  }
}
