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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.samza.Partition;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.tools.CommandLineHelper;
import org.apache.samza.tools.RandomValueGenerator;
import org.apache.samza.util.NoOpMetricsRegistry;


/**
 * Generic benchmark test for a {@link SystemProducer}.
 */
public class SystemProducerBench extends AbstractSamzaBench {

  private static final String OPT_SHORT_MESSAGE_SIZE = "sz";
  private static final String OPT_LONG_MESSAGE_SIZE = "size";
  private static final String OPT_ARG_MESSAGE_SIZE = "MESSAGE_SIZE";
  private static final String OPT_DESC_MESSAGE_SIZE = "Size of the message in bytes.";

  private byte[] value;

  public static void main(String args[]) throws Exception {
    SystemProducerBench bench = new SystemProducerBench(args);
    bench.start();
  }

  public SystemProducerBench(String args[]) throws ParseException {
    super("system-producer", args);
  }

  public void addOptions(Options options) {
    options.addOption(
        CommandLineHelper.createOption(OPT_SHORT_MESSAGE_SIZE, OPT_LONG_MESSAGE_SIZE, OPT_ARG_MESSAGE_SIZE, true,
            OPT_DESC_MESSAGE_SIZE));
  }

  public void start() throws IOException, InterruptedException {

    super.start();
    String source = "SystemProducerBench";

    int size = Integer.parseInt(cmd.getOptionValue(OPT_SHORT_MESSAGE_SIZE));
    RandomValueGenerator randGenerator = new RandomValueGenerator(System.currentTimeMillis());
    value = randGenerator.getNextString(size, size).getBytes();

    NoOpMetricsRegistry metricsRegistry = new NoOpMetricsRegistry();
    List<SystemStreamPartition> ssps = createSSPs(systemName, physicalStreamName, startPartition, endPartition);
    SystemProducer producer = factory.getProducer(systemName, config, metricsRegistry);
    producer.register(source);
    producer.start();

    System.out.println("starting production at " + Instant.now());
    Instant startTime = Instant.now();
    for (int index = 0; index < totalEvents; index++) {
      SystemStreamPartition ssp = ssps.get(index % ssps.size());
      OutgoingMessageEnvelope messageEnvelope = createMessageEnvelope(ssp, index);
      producer.send(source, messageEnvelope);
    }

    System.out.println("Ending production at " + Instant.now());
    System.out.println(String.format("Event Rate is %s Messages/Sec",
        (totalEvents * 1000 / Duration.between(startTime, Instant.now()).toMillis())));

    producer.flush(source);

    System.out.println("Ending flush at " + Instant.now());
    System.out.println(String.format("Event Rate with flush is %s Messages/Sec",
        (totalEvents * 1000 / Duration.between(startTime, Instant.now()).toMillis())));
    producer.stop();
    System.exit(0);
  }

  /**
   * Naive create message implementation that uses the same random string for each of the message.
   * If a system producer wants to test with a specific type of messages, It needs to override this method.
   */
  OutgoingMessageEnvelope createMessageEnvelope(SystemStreamPartition ssp, int index) {
    return new OutgoingMessageEnvelope(ssp.getSystemStream(), String.valueOf(index), value);
  }

  /**
   * Simple implementation to create SSPs that assumes that the partitions are ordered list of integers.
   */
  List<SystemStreamPartition> createSSPs(String systemName, String physicalStreamName, int startPartition,
      int endPartition) {
    return IntStream.range(startPartition, endPartition)
        .mapToObj(x -> new SystemStreamPartition(systemName, physicalStreamName, new Partition(x)))
        .collect(Collectors.toList());
  }
}
