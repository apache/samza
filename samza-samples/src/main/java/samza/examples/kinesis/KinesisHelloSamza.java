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

package samza.examples.kinesis;

import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.kinesis.consumer.KinesisIncomingMessageEnvelope;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskCoordinator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A sample task which consumes messages from kinesis stream and logs the message content.
 */
public class KinesisHelloSamza implements StreamTask {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisHelloSamza.class);

  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
    KinesisIncomingMessageEnvelope kEnvelope = (KinesisIncomingMessageEnvelope) envelope;
    long lagMs = System.currentTimeMillis() - kEnvelope.getApproximateArrivalTimestamp().getTime();
    LOG.info(String.format("Kinesis message key: %s Lag: %d ms", envelope.getKey(), lagMs));
  }
}