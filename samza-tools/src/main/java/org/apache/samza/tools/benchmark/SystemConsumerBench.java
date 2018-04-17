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

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.cli.ParseException;
import org.apache.samza.Partition;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemConsumer;
import org.apache.samza.system.SystemStreamMetadata;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.util.NoOpMetricsRegistry;


/**
 * Generic benchmark test for {@link SystemConsumer}.
 */
public class SystemConsumerBench extends AbstractSamzaBench {

  public static void main(String args[]) throws Exception {
    SystemConsumerBench bench = new SystemConsumerBench(args);
    bench.start();
  }

  public SystemConsumerBench(String args[]) throws ParseException {
    super(args);
  }

  public void start() throws IOException, InterruptedException {
    super.start();
    SystemAdmin systemAdmin = factory.getAdmin(systemName, config);
    SystemStreamMetadata ssm =
        systemAdmin.getSystemStreamMetadata(Collections.singleton(physicalStreamName)).get(physicalStreamName);

    NoOpMetricsRegistry metricsRegistry = new NoOpMetricsRegistry();
    Set<SystemStreamPartition> ssps = createSSPs(systemName, physicalStreamName, startPartition, endPartition);
    SystemConsumer consumer = factory.getConsumer(systemName, config, metricsRegistry);
    for (SystemStreamPartition ssp : ssps) {
      consumer.register(ssp, ssm.getSystemStreamPartitionMetadata().get(ssp.getPartition()).getOldestOffset());
    }

    consumer.start();

    System.out.println("starting consumption at " + Instant.now());
    Instant startTime = Instant.now();
    int numEvents = 0;
    while (numEvents < totalEvents) {
      Map<SystemStreamPartition, List<IncomingMessageEnvelope>> pollResult = consumer.poll(ssps, 2000);
      numEvents += pollResult.values().stream().mapToInt(List::size).sum();
    }

    System.out.println("Ending consumption at " + Instant.now());
    System.out.println(String.format("Event Rate is %s Messages/Sec ",
        (numEvents * 1000 / Duration.between(startTime, Instant.now()).toMillis())));
    consumer.stop();
    System.exit(0);
  }

  Set<SystemStreamPartition> createSSPs(String systemName, String physicalStreamName, int startPartition,
      int endPartition) {
    return IntStream.range(startPartition, endPartition)
        .mapToObj(x -> new SystemStreamPartition(systemName, physicalStreamName, new Partition(x)))
        .collect(Collectors.toSet());
  }
}
