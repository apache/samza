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
import org.apache.samza.application.StreamApplicationSpec;
import org.apache.samza.config.Config;
import org.apache.samza.operators.KV;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.runtime.ApplicationRuntime;
import org.apache.samza.runtime.ApplicationRuntimes;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.serializers.KVSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.util.CommandLine;


/**
 * Simple 2-way stream-to-stream join example
 */
public class OrderShipmentJoinExample implements StreamApplication {

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRuntime app = ApplicationRuntimes.getApplicationRuntime(new OrderShipmentJoinExample(), config);
    app.start();
    app.waitForFinish();
  }

  @Override
  public void describe(StreamApplicationSpec graph) {
    MessageStream<OrderRecord> orders =
        graph.getInputStream("orders", new JsonSerdeV2<>(OrderRecord.class));
    MessageStream<ShipmentRecord> shipments =
        graph.getInputStream("shipments", new JsonSerdeV2<>(ShipmentRecord.class));
    OutputStream<KV<String, FulfilledOrderRecord>> fulfilledOrders =
        graph.getOutputStream("fulfilledOrders",
            KVSerde.of(new StringSerde(), new JsonSerdeV2<>(FulfilledOrderRecord.class)));

    orders
        .join(shipments, new MyJoinFunction(),
            new StringSerde(), new JsonSerdeV2<>(OrderRecord.class), new JsonSerdeV2<>(ShipmentRecord.class),
            Duration.ofMinutes(1), "join")
        .map(fulFilledOrder -> KV.of(fulFilledOrder.orderId, fulFilledOrder))
        .sendTo(fulfilledOrders);
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
