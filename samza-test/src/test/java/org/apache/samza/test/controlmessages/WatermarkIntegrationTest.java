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

import org.apache.samza.application.SamzaApplication;
import scala.collection.JavaConverters;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.samza.Partition;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.container.TaskInstance;
import org.apache.samza.container.TaskName;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.descriptors.GenericInputDescriptor;
import org.apache.samza.operators.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.operators.impl.InputOperatorImpl;
import org.apache.samza.operators.impl.OperatorImpl;
import org.apache.samza.operators.impl.OperatorImplGraph;
import org.apache.samza.operators.impl.TestOperatorImpl;
import org.apache.samza.operators.spec.OperatorSpec;
import org.apache.samza.processor.StreamProcessor;
import org.apache.samza.processor.TestStreamProcessorUtil;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerdeFactory;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerdeFactory;
import org.apache.samza.standalone.PassthroughCoordinationUtilsFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.AsyncStreamTaskAdapter;
import org.apache.samza.task.StreamOperatorTask;
import org.apache.samza.task.TestStreamOperatorTask;
import org.apache.samza.test.controlmessages.TestData.PageView;
import org.apache.samza.test.controlmessages.TestData.PageViewJsonSerdeFactory;
import org.apache.samza.test.harness.AbstractIntegrationTestHarness;
import org.apache.samza.test.util.SimpleSystemAdmin;
import org.apache.samza.test.util.TestStreamConsumer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class WatermarkIntegrationTest extends AbstractIntegrationTestHarness {

  private static int offset = 1;
  private static final String TEST_SYSTEM = "test";
  private static final String TEST_STREAM = "PageView";
  private static final int PARTITION_COUNT = 2;
  private static final SystemStreamPartition SSP0 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(0));
  private static final SystemStreamPartition SSP1 = new SystemStreamPartition(TEST_SYSTEM, TEST_STREAM, new Partition(1));

  private final static List<IncomingMessageEnvelope> TEST_DATA = new ArrayList<>();
  static {
    TEST_DATA.add(createIncomingMessage(new PageView("inbox", 1), SSP0));
    TEST_DATA.add(createIncomingMessage(new PageView("home", 2), SSP1));
    TEST_DATA.add(IncomingMessageEnvelope.buildWatermarkEnvelope(SSP0, 1));
    TEST_DATA.add(IncomingMessageEnvelope.buildWatermarkEnvelope(SSP1, 2));
    TEST_DATA.add(IncomingMessageEnvelope.buildWatermarkEnvelope(SSP0, 4));
    TEST_DATA.add(IncomingMessageEnvelope.buildWatermarkEnvelope(SSP1, 3));
    TEST_DATA.add(createIncomingMessage(new PageView("search", 3), SSP0));
    TEST_DATA.add(createIncomingMessage(new PageView("pymk", 4), SSP1));
    TEST_DATA.add(IncomingMessageEnvelope.buildEndOfStreamEnvelope(SSP0));
    TEST_DATA.add(IncomingMessageEnvelope.buildEndOfStreamEnvelope(SSP1));
  }

  public final static class TestSystemFactory implements SystemFactory {
    @Override
    public SystemConsumer getConsumer(String systemName, Config config, MetricsRegistry registry) {
      return new TestStreamConsumer(TEST_DATA);
    }

    @Override
    public SystemProducer getProducer(String systemName, Config config, MetricsRegistry registry) {
      return null;
    }

    @Override
    public SystemAdmin getAdmin(String systemName, Config config) {
      return new SimpleSystemAdmin(config);
    }
  }

  private static IncomingMessageEnvelope createIncomingMessage(Object message, SystemStreamPartition ssp) {
    return new IncomingMessageEnvelope(ssp, String.valueOf(offset++), "", message);
  }

  @Test
  public void testWatermark() throws Exception {
    Map<String, String> configs = new HashMap<>();
    configs.put("app.runner.class", MockLocalApplicationRunner.class.getName());
    configs.put("systems.test.samza.factory", TestSystemFactory.class.getName());
    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.partitionCount", String.valueOf(PARTITION_COUNT));

    configs.put(JobConfig.JOB_NAME(), "test-watermark-job");
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

    configs.put("serializers.registry.int.class", IntegerSerdeFactory.class.getName());
    configs.put("serializers.registry.string.class", StringSerdeFactory.class.getName());
    configs.put("serializers.registry.json.class", PageViewJsonSerdeFactory.class.getName());

    List<PageView> received = new ArrayList<>();
    class TestStreamApp implements StreamApplication {

      @Override
      public void describe(StreamApplicationDescriptor appDesc) {
        DelegatingSystemDescriptor sd = new DelegatingSystemDescriptor("test");
        GenericInputDescriptor<KV<String, PageView>> isd =
            sd.getInputDescriptor("PageView", KVSerde.of(new NoOpSerde<>(), new NoOpSerde<>()));
        appDesc.getInputStream(isd)
            .map(EndOfStreamIntegrationTest.Values.create())
            .partitionBy(pv -> pv.getMemberId(), pv -> pv, "p1")
            .sink((m, collector, coordinator) -> {
                received.add(m.getValue());
              });
      }
    }

    final ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new TestStreamApp(), new MapConfig(configs));
    runner.run();

    // processors are only available when the app is running
    Map<String, StreamOperatorTask> tasks = getTaskOperationGraphs((MockLocalApplicationRunner) runner);

    runner.waitForFinish();
    // wait for the completion to ensure that all tasks are actually initialized and the OperatorImplGraph is initialized
    StreamOperatorTask task0 = tasks.get("Partition 0");
    OperatorImplGraph graph = TestStreamOperatorTask.getOperatorImplGraph(task0);
    OperatorImpl pb = getOperator(graph, OperatorSpec.OpCode.PARTITION_BY);
    assertEquals(TestOperatorImpl.getInputWatermark(pb), 4);
    assertEquals(TestOperatorImpl.getOutputWatermark(pb), 4);
    OperatorImpl sink = getOperator(graph, OperatorSpec.OpCode.SINK);
    assertEquals(TestOperatorImpl.getInputWatermark(sink), 3);
    assertEquals(TestOperatorImpl.getOutputWatermark(sink), 3);

    StreamOperatorTask task1 = tasks.get("Partition 1");
    graph = TestStreamOperatorTask.getOperatorImplGraph(task1);
    pb = getOperator(graph, OperatorSpec.OpCode.PARTITION_BY);
    assertEquals(TestOperatorImpl.getInputWatermark(pb), 3);
    assertEquals(TestOperatorImpl.getOutputWatermark(pb), 3);
    sink = getOperator(graph, OperatorSpec.OpCode.SINK);
    assertEquals(TestOperatorImpl.getInputWatermark(sink), 3);
    assertEquals(TestOperatorImpl.getOutputWatermark(sink), 3);
  }

  Map<String, StreamOperatorTask> getTaskOperationGraphs(MockLocalApplicationRunner runner) throws Exception {
    StreamProcessor processor = runner.getProcessors().iterator().next();
    SamzaContainer container = TestStreamProcessorUtil.getContainer(processor);
    Map<TaskName, TaskInstance> taskInstances = JavaConverters.mapAsJavaMapConverter(container.getTaskInstances()).asJava();
    Map<String, StreamOperatorTask> tasks = new HashMap<>();
    for (Map.Entry<TaskName, TaskInstance> entry : taskInstances.entrySet()) {
      AsyncStreamTaskAdapter adapter = (AsyncStreamTaskAdapter) entry.getValue().task();
      Field field = AsyncStreamTaskAdapter.class.getDeclaredField("wrappedTask");
      field.setAccessible(true);
      StreamOperatorTask task = (StreamOperatorTask) field.get(adapter);
      tasks.put(entry.getKey().getTaskName(), task);
    }
    return tasks;
  }

  OperatorImpl getOperator(OperatorImplGraph graph, OperatorSpec.OpCode opCode) {
    for (InputOperatorImpl input : graph.getAllInputOperators()) {
      Set<OperatorImpl> nextOps = TestOperatorImpl.getNextOperators(input);
      while (!nextOps.isEmpty()) {
        OperatorImpl op = nextOps.iterator().next();
        if (TestOperatorImpl.getOpCode(op) == opCode) {
          return op;
        } else {
          nextOps = TestOperatorImpl.getNextOperators(op);
        }
      }
    }
    return null;
  }

  public static class MockLocalApplicationRunner extends LocalApplicationRunner {

    /**
     * Default constructor that is required by any implementation of {@link ApplicationRunner}
     *  @param userApp user application
     * @param config user configuration
     */
    public MockLocalApplicationRunner(SamzaApplication userApp, Config config) {
      super(userApp, config);
    }

    protected Set<StreamProcessor> getProcessors() {
      return super.getProcessors();
    }
  }
}
