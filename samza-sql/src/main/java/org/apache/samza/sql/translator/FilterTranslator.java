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
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.FilterFunction;
import org.apache.samza.sql.data.Expression;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Translator to translate the LogicalFilter node in the relational graph to the corresponding StreamGraph
 * implementation
 */
class FilterTranslator {

  private static final Logger log = LoggerFactory.getLogger(FilterTranslator.class);

  private static class FilterTranslatorFunction implements FilterFunction<SamzaSqlRelMessage> {
    private transient Expression expr;
    private transient TranslatorContext context;
    private transient LogicalFilter filter;

    private final int filterId;

    FilterTranslatorFunction(int filterId) {
      this.filterId = filterId;
    }

    @Override
    public void init(Config config, TaskContext context) {
      this.context = (TranslatorContext) context.getUserContext();
      this.filter = (LogicalFilter) this.context.getRelNode(filterId);
      this.expr = this.context.getExpressionCompiler().compile(filter.getInputs(), Collections.singletonList(filter.getCondition()));
    }

    @Override
    public boolean apply(SamzaSqlRelMessage message) {
      Object[] result = new Object[1];
      expr.execute(context.getExecutionContext(), context.getDataContext(), message.getFieldValues().toArray(), result);
      if (result.length > 0 && result[0] instanceof Boolean) {
        boolean retVal = (Boolean) result[0];
        log.debug(String.format("return value for input %s is %s", Arrays.asList(message.getFieldValues()).toString(),
            retVal));
        return retVal;
      } else {
        log.error("return value is not boolean");
        return false;
      }
    }
  }

  void translate(final LogicalFilter filter, final TranslatorContext context) {
    MessageStream<SamzaSqlRelMessage> inputStream = context.getMessageStream(filter.getInput().getId());
    final int filterId = filter.getId();

    MessageStream<SamzaSqlRelMessage> outputStream = inputStream.filter(new FilterTranslatorFunction(filterId));

    context.registerMessageStream(filter.getId(), outputStream);
    context.registerRelNode(filter.getId(), filter);
  }
}
