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

import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.data.MessageEnvelope;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.serializers.StringSerde;
import org.apache.samza.runtime.ApplicationRunner;
import org.apache.samza.system.StreamSpec;
import org.apache.samza.util.CommandLine;

import java.time.Duration;

/**
 * Simple 2-way stream-to-stream join example
 */
public class OrderShipmentJoinExample implements StreamApplication {

  /**
   * used by remote application runner to launch the job in remote program. The remote program should follow the similar
   * invoking context as in local runner:
   *
   *   public static void main(String args[]) throws Exception {
   *     CommandLine cmdLine = new CommandLine();
   *     Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
   *     ApplicationRunner runner = ApplicationRunner.fromConfig(config);
   *     runner.run(new UserMainExample(), config);
   *   }
   *
   */
  @Override public void init(StreamGraph graph, Config config) {

    MessageStream<OrderRecord> orders = graph.createInStream(input1, new StringSerde("UTF-8"), new JsonSerde<>());
    MessageStream<ShipmentRecord> shipments = graph.createInStream(input2, new StringSerde("UTF-8"), new JsonSerde<>());
    OutputStream<FulFilledOrderRecord> fulfilledOrders = graph.createOutStream(output, new StringSerde("UTF-8"), new JsonSerde<>());

    orders.join(shipments, new MyJoinFunction(), Duration.ofMinutes(1)).sendTo(fulfilledOrders);

  }

  // standalone local program model
  public static void main(String[] args) throws Exception {
    CommandLine cmdLine = new CommandLine();
    Config config = cmdLine.loadConfig(cmdLine.parser().parse(args));
    ApplicationRunner localRunner = ApplicationRunner.getLocalRunner(config);
    localRunner.run(new OrderShipmentJoinExample());
  }

  StreamSpec input1 = new StreamSpec("orderStream", "OrderEvent", "kafka");

  StreamSpec input2 = new StreamSpec("shipmentStream", "ShipmentEvent", "kafka");

  StreamSpec output = new StreamSpec("joinedOrderShipmentStream", "OrderShipmentJoinEvent", "kafka");

  class OrderRecord implements MessageEnvelope<String, OrderRecord> {
    String orderId;
    long orderTimeMs;

    OrderRecord(String orderId, long timeMs) {
      this.orderId = orderId;
      this.orderTimeMs = timeMs;
    }

    @Override
    public String getKey() {
      return this.orderId;
    }

    @Override
    public OrderRecord getMessage() {
      return this;
    }
  }

  class ShipmentRecord implements MessageEnvelope<String, ShipmentRecord> {
    String orderId;
    long shipTimeMs;

    ShipmentRecord(String orderId, long timeMs) {
      this.orderId = orderId;
      this.shipTimeMs = timeMs;
    }

    @Override
    public String getKey() {
      return this.orderId;
    }

    @Override
    public ShipmentRecord getMessage() {
      return this;
    }
  }

  class FulFilledOrderRecord implements MessageEnvelope<String, FulFilledOrderRecord> {
    String orderId;
    long orderTimeMs;
    long shipTimeMs;

    FulFilledOrderRecord(String orderId, long orderTimeMs, long shipTimeMs) {
      this.orderId = orderId;
      this.orderTimeMs = orderTimeMs;
      this.shipTimeMs = shipTimeMs;
    }


    @Override
    public String getKey() {
      return this.orderId;
    }

    @Override
    public FulFilledOrderRecord getMessage() {
      return this;
    }
  }

  FulFilledOrderRecord myJoinResult(OrderRecord m1, ShipmentRecord m2) {
    return new FulFilledOrderRecord(m1.getKey(), m1.orderTimeMs, m2.shipTimeMs);
  }

  class MyJoinFunction implements JoinFunction<String, OrderRecord, ShipmentRecord, FulFilledOrderRecord> {

    @Override
    public FulFilledOrderRecord apply(OrderRecord message, ShipmentRecord otherMessage) {
      return OrderShipmentJoinExample.this.myJoinResult(message, otherMessage);
    }

    @Override
    public String getFirstKey(OrderRecord message) {
      return message.getKey();
    }

    @Override
    public String getSecondKey(ShipmentRecord message) {
      return message.getKey();
    }
  }
}
