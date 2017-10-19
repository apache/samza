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

package org.apache.samza.test.operator;

import java.io.IOException;
import java.util.stream.Stream;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplications;
import org.apache.samza.config.Config;
import org.apache.samza.config.KafkaConfig;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.system.kafka.KafkaSystem;
import org.apache.samza.util.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;

import static org.apache.samza.operators.StreamDescriptor.*;


/**
 * A {@link org.apache.samza.application.StreamApplication} that demonstrates a repartition followed by a windowed count.
 */
public class RepartitionWindowApp {

  private static final Logger LOG = LoggerFactory.getLogger(RepartitionWindowApp.class);

  public static void main(String[] args) throws IOException {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));

    KafkaSystem kafka = KafkaSystem.create("kafka").withBootstrapServers(config.get("systems.kafka.producer.bootstrap.servers"));
    StreamDescriptor.Input<String, String> pveInput = StreamDescriptor.<String, String>input("page-views").from(kafka);
    StreamDescriptor.Output<String, String> cntOutput = StreamDescriptor.<String, String>output("Result").from(kafka);

    StreamApplication reparApp = StreamApplications.createStreamApp(config).withDefaultSystem(kafka);

    MapFunction<String, String> keyFn = pageView -> new PageView(pageView).getUserId();
    reparApp.openInput(pveInput)
        .partitionBy(keyFn)
        .window(Windows.keyedSessionWindow(keyFn, Duration.ofSeconds(3)))
        .sendTo(reparApp.openOutput(cntOutput, m -> m.getKey().getKey(), m -> new Integer(m.getMessage().size()).toString()));

    reparApp.run();
    reparApp.waitForFinish();
  }
}
