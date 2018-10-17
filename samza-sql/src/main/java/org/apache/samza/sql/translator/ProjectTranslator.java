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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.samza.SamzaException;
import org.apache.samza.context.Context;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.sql.data.Expression;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Translator to translate the Project node in the relational graph to the corresponding StreamGraph
 * implementation.
 */
class ProjectTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(ProjectTranslator.class);
  private final int queryId;
  ProjectTranslator(int queryId) {
    this.queryId = queryId;
  }

  private static class ProjectMapFunction implements MapFunction<SamzaSqlRelMessage, SamzaSqlRelMessage> {
    private transient Project project;
    private transient Expression expr;
    private transient TranslatorContext context;

    private final int queryId;
    private final int projectId;

    ProjectMapFunction(int projectId, int queryId) {
      this.projectId = projectId;
      this.queryId = queryId;
    }

    @Override
    public void init(Context context) {
      this.context = ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      this.project = (Project) this.context.getRelNode(projectId);
      this.expr = this.context.getExpressionCompiler().compile(project.getInputs(), project.getProjects());
    }

    @Override
    public SamzaSqlRelMessage apply(SamzaSqlRelMessage message) {
      RelDataType type = project.getRowType();
      Object[] output = new Object[type.getFieldCount()];
      expr.execute(context.getExecutionContext(), context.getDataContext(),
          message.getSamzaSqlRelRecord().getFieldValues().toArray(), output);
      List<String> names = new ArrayList<>();
      for (int index = 0; index < output.length; index++) {
        names.add(index, project.getNamedProjects().get(index).getValue());
      }

      return new SamzaSqlRelMessage(names, Arrays.asList(output));
    }
  }

  private MessageStream<SamzaSqlRelMessage> translateFlatten(Integer flattenIndex,
      MessageStream<SamzaSqlRelMessage> inputStream) {
    return inputStream.flatMap(message -> {
      Object field = message.getSamzaSqlRelRecord().getFieldValues().get(flattenIndex);

      if (field != null && field instanceof List) {
        List<SamzaSqlRelMessage> outMessages = new ArrayList<>();
        for (Object fieldValue : (List) field) {
          List<Object> newValues = new ArrayList<>(message.getSamzaSqlRelRecord().getFieldValues());
          newValues.set(flattenIndex, Collections.singletonList(fieldValue));
          outMessages.add(new SamzaSqlRelMessage(message.getSamzaSqlRelRecord().getFieldNames(), newValues));
        }
        return outMessages;
      } else {
        return Collections.singletonList(message);
      }
    });
  }

  private boolean isFlatten(RexNode rexNode) {
    return rexNode instanceof RexCall && ((RexCall) rexNode).op instanceof SqlUserDefinedFunction
        && ((RexCall) rexNode).op.getName().equalsIgnoreCase("flatten");
  }

  private Integer getProjectIndex(RexNode rexNode) {
    return ((RexInputRef) ((RexCall) rexNode).getOperands().get(0)).getIndex();
  }

  void translate(final Project project, final TranslatorContext context) {
    MessageStream<SamzaSqlRelMessage> messageStream = context.getMessageStream(project.getInput().getId());
    List<Integer> flattenProjects =
        project.getProjects().stream().filter(this::isFlatten).map(this::getProjectIndex).collect(Collectors.toList());

    if (flattenProjects.size() > 0) {
      if (flattenProjects.size() > 1) {
        String msg = "Multiple flatten operators in a single query is not supported";
        LOG.error(msg);
        throw new SamzaException(msg);
      }
      messageStream = translateFlatten(flattenProjects.get(0), messageStream);
    }

    final int projectId = project.getId();

    MessageStream<SamzaSqlRelMessage> outputStream = messageStream.map(new ProjectMapFunction(projectId, queryId));

    context.registerMessageStream(project.getId(), outputStream);
    context.registerRelNode(project.getId(), project);
  }

}
