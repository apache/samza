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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigRewriter;
import org.apache.samza.config.MapConfig;
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
import org.apache.samza.test.framework.TestRunner;
import org.apache.samza.test.framework.system.descriptors.InMemoryInputDescriptor;
import org.apache.samza.test.framework.system.descriptors.InMemorySystemDescriptor;

import org.junit.Test;

import static org.apache.samza.test.table.TestLocalTableWithLowLevelApiEndToEnd.MyStreamTask;


public class TestLocalTableWithConfigRewriterEndToEnd {
  /**
   * MyTaskApplication does not include table descriptor, so if the rewriter does not add the table configs properly,
   * then the application will fail to execute.
   */
  @Test
  public void testWithConfigRewriter() {
    InMemorySystemDescriptor isd = new InMemorySystemDescriptor("test");
    InMemoryInputDescriptor<TestTableData.PageView> inputDescriptor = isd
        .getInputDescriptor("PageView", new NoOpSerde<>());
    TestRunner.of(new MyTaskApplication())
        .addInputStream(inputDescriptor, TestTableData.generatePartitionedPageViews(40, 4))
        .addConfig("job.config.rewriters", "my-rewriter")
        .addConfig("job.config.rewriter.my-rewriter.class", MyConfigRewriter.class.getName())
        .run(Duration.ofSeconds(10));
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
