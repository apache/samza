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

import java.time.Duration;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.context.Context;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.table.ReadWriteUpdateTable;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.junit.Assert;
import org.junit.Test;


public class TestLocalTableWithLowLevelApiEndToEnd {
  @Test
  public void testTableWithLowLevelApi() {
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd
        .getInputDescriptor("PageView", new NoOpSerde<>());
    TestRunner.of(new TestLocalTableWithConfigRewriterEndToEnd.MyTaskApplication())
        .addInputStream(inputDescriptor, TestTableData.generatePartitionedPageViews(40, 4))
        .addConfig("job.config.rewriters", "my-rewriter")
        .addConfig("job.config.rewriter.my-rewriter.class", TestLocalTableWithConfigRewriterEndToEnd.MyConfigRewriter.class.getName())
        .run(Duration.ofSeconds(10));
  }

  static public class MyTaskApplication implements TaskApplication {
    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {
      DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
      GenericInputDescriptor<TestTableData.PageView> pageViewISD = ksd.getInputDescriptor("PageView", new NoOpSerde<>());
      appDescriptor
          .withInputStream(pageViewISD)
          .withTable(new InMemoryTableDescriptor("t1", KVSerde.of(new IntegerSerde(), new TestTableData.PageViewJsonSerde())))
          .withTaskFactory((StreamTaskFactory) () -> new MyStreamTask());
    }
  }

  static public class MyStreamTask implements StreamTask, InitableTask {
    private ReadWriteUpdateTable<Integer, TestTableData.PageView, ?> pageViewTable;
    @Override
    public void init(Context context) {
      pageViewTable = context.getTaskContext().getUpdatableTable("t1");
    }
    @Override
    public void process(IncomingMessageEnvelope message, MessageCollector collector, TaskCoordinator coordinator) {
      TestTableData.PageView pv = (TestTableData.PageView) message.getMessage();
      pageViewTable.put(pv.getMemberId(), pv);
      TestTableData.PageView pv2 = pageViewTable.get(pv.getMemberId());
      Assert.assertEquals(pv.getMemberId(), pv2.getMemberId());
      Assert.assertEquals(pv.getPageKey(), pv2.getPageKey());
    }
  }
}
