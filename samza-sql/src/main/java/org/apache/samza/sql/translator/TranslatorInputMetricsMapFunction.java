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

package org.apache.samza.sql.translator;

import java.time.Instant;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.sql.data.SamzaSqlRelMessage;


/**
 * TranslatorInputMetricsMapFunction is a dummy map function to maintain join metrics at Input
 */
class TranslatorInputMetricsMapFunction implements MapFunction<SamzaSqlRelMessage, SamzaSqlRelMessage> {
  private transient MetricsRegistry metricsRegistry;
  private transient Counter inputEvents;

  private final String logicalOpId;

  TranslatorInputMetricsMapFunction(String logicalOpId) { this.logicalOpId = logicalOpId; }

  /**
   * initializes the TranslatorOutputMetricsMapFunction before any message is processed
   * @param context the {@link Context} for this task
   */
  @Override
  public void init(Context context) {
    ContainerContext containerContext = context.getContainerContext();
    metricsRegistry = containerContext.getContainerMetricsRegistry();
    inputEvents = metricsRegistry.newCounter(logicalOpId, TranslatorConstants.INPUT_EVENTS_NAME);
    inputEvents.clear();
  }

  /**
   * update metrics given a message
   * @param message  the input message
   * @return the same message
   */
  @Override
  public SamzaSqlRelMessage apply(SamzaSqlRelMessage message) {
    inputEvents.inc();
    message.getSamzaSqlRelMsgMetadata().joinStartTimeMs = Instant.now().toEpochMilli();
    return message;
  }

} // TranslatorInputMetricsMapFunction
