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

package org.apache.samza.tools.benchmark;

import com.google.common.base.Joiner;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.cli.ParseException;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.ApplicationConfig;
import org.apache.samza.config.SystemConfig;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.system.descriptors.GenericInputDescriptor;
import org.apache.samza.system.descriptors.GenericSystemDescriptor;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.serializers.NoOpSerde;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;


/**
 * Generic benchmark test for a test consumer but with samza framework
 */
public class SystemConsumerWithSamzaBench extends AbstractSamzaBench {
  public SystemConsumerWithSamzaBench(String[] args) throws ParseException {
    super("system-consumer-with-samza-bench", args);
  }

  public static void main(String args[]) throws Exception {
    SystemConsumerBench bench = new SystemConsumerBench(args);
    bench.start();
  }

  @Override
  public void addMoreSystemConfigs(Properties props) {
    props.put("app.runner.class", LocalApplicationRunner.class.getName());
    List<Integer> partitions = IntStream.range(startPartition, endPartition).boxed().collect(Collectors.toList());
    props.put(ApplicationConfig.APP_NAME, "SamzaBench");
    props.put(JobConfig.PROCESSOR_ID(), "1");
    props.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    props.put(String.format(ConfigBasedSspGrouperFactory.CONFIG_STREAM_PARTITIONS, streamId),
        Joiner.on(",").join(partitions));
    props.put(TaskConfig.GROUPER_FACTORY(), ConfigBasedSspGrouperFactory.class.getName());
  }

  public void start() throws IOException, InterruptedException {
    super.start();
    MessageConsumer consumeFn = new MessageConsumer();
    StreamApplication app = appDesc -> {
      String systemFactoryName = new SystemConfig(config).getSystemFactory(systemName).get();
      GenericSystemDescriptor sd = new GenericSystemDescriptor(systemName, systemFactoryName);
      GenericInputDescriptor<Object> isd = sd.getInputDescriptor(streamId, new NoOpSerde<>());
      MessageStream<Object> stream = appDesc.getInputStream(isd);
      stream.map(consumeFn);
    };
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(app, new MapConfig());

    runner.run();

    while (consumeFn.getEventsConsumed() < totalEvents) {
      Thread.sleep(10);
    }

    Instant endTime = Instant.now();

    runner.kill();

    System.out.println("\n*******************");
    System.out.println(String.format("Started at %s Ending at %s ", consumeFn.startTime, endTime));
    System.out.println(String.format("Event Rate is %s Messages/Sec ",
        (consumeFn.getEventsConsumed() * 1000 / Duration.between(consumeFn.startTime, Instant.now()).toMillis())));

    System.out.println(
        "Event Rate is " + consumeFn.getEventsConsumed() * 1000 / Duration.between(consumeFn.startTime, endTime).toMillis());
    System.out.println("*******************\n");

    System.exit(0);
  }

  private class MessageConsumer implements MapFunction<Object, Object> {
    AtomicInteger eventsConsumed = new AtomicInteger(0);
    volatile Instant startTime;

    @Override
    public Object apply(Object message) {

      eventsConsumed.incrementAndGet();
      if (eventsConsumed.get() == 1) {
        startTime = Instant.now();
      }
      return message;
    }

    public int getEventsConsumed() {
      return eventsConsumed.get();
    }
  }
}

