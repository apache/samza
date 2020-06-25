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

import java.util.Arrays;
import java.util.Collections;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.samza.SamzaException;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SamzaHistogram;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.sql.data.Expression;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Translator to translate the LogicalFilter node in the relational graph to the corresponding StreamGraph
 * implementation
 */
class FilterTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(FilterTranslator.class);
  private final int queryId;

  FilterTranslator(int queryId) {
    this.queryId = queryId;
  }

  /**
   * FilterTranslatorFunction to process input events, apply the filter and produce output
   * events accordingly
   */
  private static class FilterTranslatorFunction implements FilterFunction<SamzaSqlRelMessage> {
    private transient Expression expr;
    private transient TranslatorContext translatorContext;
    private transient LogicalFilter filter;
    private transient MetricsRegistry metricsRegistry;
    private transient SamzaHistogram processingTime; // milli-seconds
    private transient Counter inputEvents;
    private transient Counter filteredOutEvents;
    private transient Counter outputEvents;

    private final int queryId;
    private final int filterId;
    private final String logicalOpId;
    private Context context;

    FilterTranslatorFunction(int filterId, int queryId, String logicalOpId) {
      this.filterId = filterId;
      this.queryId = queryId;
      this.logicalOpId = logicalOpId;
    }

    @Override
    public void init(Context context) {
      this.context = context;
      this.translatorContext = ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      this.filter = (LogicalFilter) this.translatorContext.getRelNode(filterId);
      this.expr = this.translatorContext.getExpressionCompiler().compile(filter.getInputs(), Collections.singletonList(filter.getCondition()));
      ContainerContext containerContext = context.getContainerContext();
      metricsRegistry = containerContext.getContainerMetricsRegistry();
      processingTime = new SamzaHistogram(metricsRegistry, logicalOpId, TranslatorConstants.PROCESSING_TIME_NAME);
      inputEvents = metricsRegistry.newCounter(logicalOpId, TranslatorConstants.INPUT_EVENTS_NAME);
      inputEvents.clear();
      filteredOutEvents = metricsRegistry.newCounter(logicalOpId, TranslatorConstants.FILTERED_EVENTS_NAME);
      filteredOutEvents.clear();
      outputEvents = metricsRegistry.newCounter(logicalOpId, TranslatorConstants.OUTPUT_EVENTS_NAME);
      outputEvents.clear();
    }

    @Override
    public boolean apply(SamzaSqlRelMessage message) {
      long startProcessing = System.nanoTime();
      Object[] result = new Object[1];
      try {
        expr.execute(translatorContext.getExecutionContext(), context, translatorContext.getDataContext(),
            message.getSamzaSqlRelRecord().getFieldValues().toArray(), result);
      } catch (Exception e) {
        String errMsg = String.format("Handling the following rel message ran into an error. %s", message);
        LOG.error(errMsg, e);
        throw new SamzaException(errMsg, e);
      }
      if (result[0] == null) {
        // Case filter is applied on a null value -> result is neither true or false.
        // Samza Filter operator supports primitive return types only, return false as per current convention.
        return false;
      }
      if (result[0] instanceof Boolean) {
        boolean retVal = (Boolean) result[0];
        LOG.debug(
            String.format("return value for input %s is %s",
                Arrays.asList(message.getSamzaSqlRelRecord().getFieldValues()).toString(), retVal));
        updateMetrics(startProcessing, retVal, System.nanoTime());
        return retVal;
      } else {
        LOG.error("return value is not boolean for rel message: {}", message);
        return false;
      }
    }

    /**
     * Updates the MetricsRegistery of this operator
     * @param startProcessing = begin processing of the message
     * @param endProcessing = end of processing
     */
    private void updateMetrics(long startProcessing, boolean isOutput, long endProcessing) {
      inputEvents.inc();
      if (isOutput) {
        outputEvents.inc();
      } else {
        filteredOutEvents.inc();
      }
      processingTime.update(endProcessing - startProcessing);
    }

  }

  void translate(final LogicalFilter filter, final String logicalOpId, final TranslatorContext context) {
    MessageStream<SamzaSqlRelMessage> inputStream = context.getMessageStream(filter.getInput().getId());
    final int filterId = filter.getId();

    MessageStream<SamzaSqlRelMessage> outputStream = inputStream.filter(new FilterTranslatorFunction(filterId, queryId, logicalOpId));

    context.registerMessageStream(filterId, outputStream);
    context.registerRelNode(filterId, filter);
  }
}
