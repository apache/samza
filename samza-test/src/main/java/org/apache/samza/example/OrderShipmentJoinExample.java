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

import java.time.Duration;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.StreamApplicationDescriptor;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.runtime.ApplicationRunners;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.system.kafka.KafkaInputDescriptor;
import org.apache.samza.system.kafka.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.KafkaSystemDescriptor;
import org.apache.samza.util.CommandLine;


/**
 * Simple 2-way stream-to-stream join example
 */
public class OrderShipmentJoinExample implements StreamApplication {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner runner = ApplicationRunners.getApplicationRunner(new OrderShipmentJoinExample(), config);
    runner.run();
    runner.waitForFinish();
  }

  @Override
  public void describe(StreamApplicationDescriptor appDesc) {
    KafkaSystemDescriptor trackingSystem = new KafkaSystemDescriptor("tracking");

    KafkaInputDescriptor<OrderRecord> orderStreamDescriptor =
        trackingSystem.getInputDescriptor("orders", new JsonSerdeV2<>(OrderRecord.class));
    KafkaInputDescriptor<ShipmentRecord> shipmentStreamDescriptor =
        trackingSystem.getInputDescriptor("shipments", new JsonSerdeV2<>(ShipmentRecord.class));
    KafkaOutputDescriptor<KV<String, FulfilledOrderRecord>> fulfilledOrdersStreamDescriptor =
        trackingSystem.getOutputDescriptor("fulfilledOrders",
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(FulfilledOrderRecord.class)));

    appDesc.getInputStream(orderStreamDescriptor)
        .join(appDesc.getInputStream(shipmentStreamDescriptor), new MyJoinFunction(),
            new StringSerde(), new JsonSerdeV2<>(OrderRecord.class), new JsonSerdeV2<>(ShipmentRecord.class),
            Duration.ofMinutes(1), "join")
        .map(fulFilledOrder -> KV.of(fulFilledOrder.orderId, fulFilledOrder))
        .sendTo(appDesc.getOutputStream(fulfilledOrdersStreamDescriptor));

  }

  static class MyJoinFunction implements JoinFunction<String, OrderRecord, ShipmentRecord, FulfilledOrderRecord> {
    @Override
    public FulfilledOrderRecord apply(OrderRecord message, ShipmentRecord otherMessage) {
      return new FulfilledOrderRecord(message.orderId, message.orderTimeMs, otherMessage.shipTimeMs);
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

  static class FulfilledOrderRecord {
    String orderId;
    long orderTimeMs;
    long shipTimeMs;

    FulfilledOrderRecord(String orderId, long orderTimeMs, long shipTimeMs) {
      this.orderId = orderId;
      this.orderTimeMs = orderTimeMs;
      this.shipTimeMs = shipTimeMs;
    }
  }
}
