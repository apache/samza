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

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.serializers.JsonSerdeV2;
import org.apache.samza.test.operator.data.PageView;
import org.apache.samza.test.util.StreamAssert;

import java.util.Arrays;

import static org.apache.samza.test.operator.RepartitionJoinWindowApp.PAGE_VIEWS;

public class BroadcastAssertApp implements StreamApplication {

  @Override
  public void init(StreamGraph graph, Config config) {
    final JsonSerdeV2<PageView> serde = new JsonSerdeV2<>(PageView.class);
    final MessageStream<PageView> broadcastPageViews = graph
        .getInputStream(PAGE_VIEWS, serde)
        .broadcast(serde, "pv");

    /**
     * Each task will see all the pageview events
     */
    StreamAssert.that("Each task contains all broadcast PageView events", broadcastPageViews, serde)
        .forEachTask()
        .containsInAnyOrder(
            Arrays.asList(
                new PageView("v1", "p1", "u1"),
                new PageView("v2", "p2", "u1"),
                new PageView("v3", "p1", "u2"),
                new PageView("v4", "p3", "u2")
            ));
  }
}
