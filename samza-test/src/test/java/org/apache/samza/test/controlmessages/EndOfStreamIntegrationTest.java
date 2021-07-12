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

package org.apache.samza.test.controlmessages;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.descriptors.StreamApplicationDescriptor;
import org.apache.samza.operators.KV;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.test.table.TestTableData.PageView;
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;
import org.apache.samza.test.table.TestTableData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This test uses an array as a bounded input source, and does a partitionBy() and sink() after reading the input.
 * It verifies the pipeline will stop and the number of output messages should equal to the input.
 */
public class EndOfStreamIntegrationTest {
  private static final List<PageView> RECEIVED = new ArrayList<>();

  @Test
  public void testPipeline() {
    class PipelineApplication implements StreamApplication {
      @Override
      public void describe(StreamApplicationDescriptor appDescriptor) {
        DelegatingSystemDescriptor sd = new DelegatingSystemDescriptor("test");
        GenericInputDescriptor<KV<String, PageView>> isd =
            sd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
        appDescriptor.getInputStream(isd)
            .map(KV::getValue)
            .partitionBy(PageView::getMemberId, pv -> pv,
                KVSerde.of(new IntegerSerde(), new TestTableData.PageViewJsonSerde()), "p1")
            .sink((m, collector, coordinator) -> {
              RECEIVED.add(m.getValue());
            });
      }
    }

    int numPageViews = 40;
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd
        .getInputDescriptor("PageView", new NoOpSerde<>());
    TestRunner.of(new PipelineApplication())
        .addInputStream(inputDescriptor, TestTableData.generatePartitionedPageViews(numPageViews, 4))
        .run(Duration.ofSeconds(10));

    assertEquals(RECEIVED.size(), numPageViews);
  }
}
