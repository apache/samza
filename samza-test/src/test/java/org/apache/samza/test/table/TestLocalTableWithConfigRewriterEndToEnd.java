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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.MapConfig;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.IntegerSerde;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.storage.kv.inmemory.descriptors.InMemoryTableDescriptor;
import org.apache.samza.system.descriptors.DelegatingSystemDescriptor;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.table.TableConfigGenerator;
import org.apache.samza.table.descriptors.TableDescriptor;
import org.apache.samza.task.StreamTaskFactory;
import org.apache.samza.test.harness.IntegrationTestHarness;
import org.apache.samza.test.util.Base64Serializer;

import org.junit.Test;

import static org.apache.samza.test.table.TestLocalTableEndToEnd.getBaseJobConfig;
import static org.apache.samza.test.table.TestLocalTableWithLowLevelApiEndToEnd.MyStreamTask;


public class TestLocalTableWithConfigRewriterEndToEnd extends IntegrationTestHarness {

  @Test
  public void testWithConfigRewriter() throws Exception {
    Map<String, String> configs = getBaseJobConfig(bootstrapUrl(), zkConnect());
    configs.put("streams.PageView.samza.system", "test");
    configs.put("streams.PageView.source", Base64Serializer.serialize(TestTableData.generatePageViews(10)));
    configs.put("streams.PageView.partitionCount", String.valueOf(4));
    configs.put("task.inputs", "test.PageView");
    configs.put("job.config.rewriter.my-rewriter.class", MyConfigRewriter.class.getName());
    configs.put("job.config.rewriters", "my-rewriter");

    Config config = new MapConfig(configs);
    final LocalApplicationRunner runner = new LocalApplicationRunner(new MyTaskApplication(), config);
    executeRun(runner, config);
    runner.waitForFinish();
  }

  static public class MyConfigRewriter implements ConfigRewriter {
    @Override
    public Config rewrite(String name, Config config) {
      List<TableDescriptor> descriptors = Arrays.asList(
          new InMemoryTableDescriptor("t1", KVSerde.of(new IntegerSerde(), new TestTableData.PageViewJsonSerde())),
          new InMemoryTableDescriptor("t2", KVSerde.of(new IntegerSerde(), new StringSerde())));
      Map<String, String> serdeConfig = TableConfigGenerator.generateSerdeConfig(descriptors);
      Map<String, String> tableConfig = TableConfigGenerator.generate(new MapConfig(config, serdeConfig), descriptors);
      return new MapConfig(config, serdeConfig, tableConfig);
    }
  }

  static public class MyTaskApplication implements TaskApplication {
    @Override
    public void describe(TaskApplicationDescriptor appDescriptor) {
      DelegatingSystemDescriptor ksd = new DelegatingSystemDescriptor("test");
      GenericInputDescriptor<TestTableData.PageView> pageViewISD = ksd.getInputDescriptor("PageView", new NoOpSerde<>());
      appDescriptor
          .withInputStream(pageViewISD)
          .withTaskFactory((StreamTaskFactory) () -> new MyStreamTask());
    }
  }
}
