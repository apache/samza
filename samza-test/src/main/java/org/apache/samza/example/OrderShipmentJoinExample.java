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
package org.apache.samza.example;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.system.kafka.KafkaSystem;
import org.apache.samza.operators.StreamDescriptor;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.util.CommandLine;

import java.time.Duration;

/**
 * Simple 2-way stream-to-stream join example
 */
public class OrderShipmentJoinExample {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));

    KafkaSystem kafkaSystem = KafkaSystem.create("kafka")
        .withBootstrapServers("localhost:9192")
        .withConsumerProperties(config)
        .withProducerProperties(config);

    StreamDescriptor.Input<String, OrderRecord> orders = StreamDescriptor.<String, OrderRecord>input("orderStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);
    StreamDescriptor.Input<String, ShipmentRecord> shipments = StreamDescriptor.<String, ShipmentRecord>input("shipmentStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);
    StreamDescriptor.Output<String, FulFilledOrderRecord> fulfilledOrders = StreamDescriptor.<String, FulFilledOrderRecord>output("joinedOrderShipmentStream")
        .withKeySerde(new StringSerde("UTF-8"))
        .withMsgSerde(new JsonSerde<>())
        .from(kafkaSystem);

    StreamApplication app = StreamApplication.create(config);
    app.open(orders)
        .join(app.open(shipments), new MyJoinFunction(), Duration.ofMinutes(1))
        .sendTo(app.open(fulfilledOrders, m -> m.orderId));

    app.run();
    app.waitForFinish();
  }

  static class MyJoinFunction implements JoinFunction<String, OrderRecord, ShipmentRecord, FulFilledOrderRecord> {
    @Override
    public FulFilledOrderRecord apply(OrderRecord message, ShipmentRecord otherMessage) {
      return new FulFilledOrderRecord(message.orderId, message.orderTimeMs, otherMessage.shipTimeMs);
    }

    @Override
    public String getFirstKey(OrderRecord message) {
      return message.orderId;
    }

    @Override
    public String getSecondKey(ShipmentRecord message) {
      return message.orderId;
    }
  }

  class OrderRecord {
    String orderId;
    long orderTimeMs;

    OrderRecord(String orderId, long timeMs) {
      this.orderId = orderId;
      this.orderTimeMs = timeMs;
    }
  }

  class ShipmentRecord {
    String orderId;
    long shipTimeMs;

    ShipmentRecord(String orderId, long timeMs) {
      this.orderId = orderId;
      this.shipTimeMs = timeMs;
    }
  }

  static class FulFilledOrderRecord {
    String orderId;
    long orderTimeMs;
    long shipTimeMs;

    FulFilledOrderRecord(String orderId, long orderTimeMs, long shipTimeMs) {
      this.orderId = orderId;
      this.orderTimeMs = orderTimeMs;
      this.shipTimeMs = shipTimeMs;
    }
  }
}
