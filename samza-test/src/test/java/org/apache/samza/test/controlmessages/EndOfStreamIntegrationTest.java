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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.test.controlmessages.TestData.PageView;
import org.apache.samza.test.controlmessages.TestData.PageViewJsonSerdeFactory;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.util.ArraySystemFactory;
import org.apache.samza.test.util.Base64Serializer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This test uses an array as a bounded input source, and does a partitionBy() and sink() after reading the input.
 * It verifies the pipeline will stop and the number of output messages should equal to the input.
 */
public class EndOfStreamIntegrationTest extends AbstractIntegrationTestHarness {

  private static final String[] PAGEKEYS = {"inbox", "home", "search", "pymk", "group", "job"};

  private static List<PageView> received = new ArrayList<>();

  @Test
  public void testPipeline() throws  Exception {
    Random random = new Random();
    int count = 10;
    PageView[] pageviews = new PageView[count];
    for (int i = 0; i < count; i++) {
      String pagekey = PAGEKEYS[random.nextInt(PAGEKEYS.length - 1)];
      int memberId = random.nextInt(10);
      pageviews[i] = new PageView(pagekey, memberId);
    }

    int partitionCount = 4;
    Map<String, String> configs = new HashMap<>();
    configs.put("app.runner.class", "org.apache.samza.runtime.LocalApplicationRunner");
    configs.put("systems.test.samza.factory", ArraySystemFactory.class.getName());
    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(pageviews));
    configs.put("streams.PageView.partitionCount", String.valueOf(partitionCount));

    configs.put(JobConfig.JOB_NAME(), "test-eos-job");
    configs.put(JobConfig.PROCESSOR_ID(), "1");
    configs.put(JobCoordinatorConfig.JOB_COORDINATION_UTILS_FACTORY, PassthroughCoordinationUtilsFactory.class.getName());
    configs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    configs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

    configs.put("systems.kafka.samza.factory", "org.apache.samza.system.kafka.KafkaSystemFactory");
    configs.put("systems.kafka.producer.bootstrap.servers", bootstrapUrl());
    configs.put("systems.kafka.consumer.zookeeper.connect", zkConnect());
    configs.put("systems.kafka.samza.key.serde", "int");
    configs.put("systems.kafka.samza.msg.serde", "json");
    configs.put("systems.kafka.default.stream.replication.factor", "1");
    configs.put("job.default.system", "kafka");

    configs.put("serializers.registry.int.class", "org.apache.samza.serializers.IntegerSerdeFactory");
    configs.put("serializers.registry.json.class", PageViewJsonSerdeFactory.class.getName());

    class PipelineApplication implements StreamApplication {

      @Override
      public void describe(StreamApplicationDescriptor appDesc) {
        DelegatingSystemDescriptor sd = new DelegatingSystemDescriptor("test");
        GenericInputDescriptor<KV<String, PageView>> isd =
            sd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
        appDesc.getInputStream(isd)
            .map(Values.create())
            .partitionBy(pv -> pv.getMemberId(), pv -> pv, "p1")
            .sink((m, collector, coordinator) -> {
                received.add(m.getValue());
              });
      }
    }

    final ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new PipelineApplication(), new MapConfig(configs));

    runner.run();
    runner.waitForFinish();

    assertEquals(received.size(), count * partitionCount);
  }

  public static final class Values {
    public static <K, V, M extends KV<K, V>> MapFunction<M, V> create() {
      return (M m) -> m.getValue();
    }
  }
}
