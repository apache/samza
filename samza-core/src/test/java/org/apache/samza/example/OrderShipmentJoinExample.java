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
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.util.CommandLine;

import java.time.Duration;

/**
 * Simple 2-way stream-to-stream join example
 */
public class OrderShipmentJoinExample implements StreamApplication {

  @Override
  public void init(StreamGraph graph, Config config) {
    MessageStream<OrderRecord> orders = graph.getInputStream("orderStream", (k, m) -> (OrderRecord) m);
    MessageStream<ShipmentRecord> shipments = graph.getInputStream("shipmentStream", (k, m) -> (ShipmentRecord) m);
    OutputStream<String, FulFilledOrderRecord, FulFilledOrderRecord> joinedOrderShipmentStream =
        graph.getOutputStream("joinedOrderShipmentStream", m -> m.orderId, m -> m);

    orders
        .join(shipments, new MyJoinFunction(), Duration.ofMinutes(1))
        .sendTo(joinedOrderShipmentStream);
  }

  // local execution mode
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    localRunner.run(new OrderShipmentJoinExample());
  }

  class MyJoinFunction implements JoinFunction<String, OrderRecord, ShipmentRecord, FulFilledOrderRecord> {
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

  class FulFilledOrderRecord {
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
