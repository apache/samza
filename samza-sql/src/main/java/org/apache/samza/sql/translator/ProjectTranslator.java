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

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.DataContext;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.apache.samza.SamzaException;
import org.apache.samza.context.ContainerContext;
import org.apache.samza.context.Context;
import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.SamzaHistogram;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.functions.MapFunction;
import org.apache.samza.sql.SamzaSqlRelRecord;
import org.apache.samza.sql.data.Expression;
import org.apache.samza.sql.data.SamzaSqlExecutionContext;
import org.apache.samza.sql.data.SamzaSqlRelMessage;
import org.apache.samza.sql.data.SamzaSqlRelMsgMetadata;
import org.apache.samza.sql.runner.SamzaSqlApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Translator to translate the Project node in the relational graph to the corresponding StreamGraph
 * implementation.
 */
public class ProjectTranslator {

  private static final Logger LOG = LoggerFactory.getLogger(ProjectTranslator.class);
  //private transient int messageIndex = 0;
  private final int queryId;

  ProjectTranslator(int queryId) {
    this.queryId = queryId;
  }

  /**
   * Converts the resulting row from Calcite Expression Evaluator to SamzaRelRecord to be sent downstream.
   *
   * @param objects input objects to be converted
   * @param rowType Calcite row type of the resulting row
   * @return return a valid message Stream of type SamzaSqlRelRecord
   */
  public static SamzaSqlRelRecord buildSamzaRelRecord(Object[] objects, RelDataType rowType) {
    Preconditions.checkNotNull(objects, "Input objects can not be null");
    Preconditions.checkState(rowType.isStruct(), "Row Type has to be a Struct and got " + rowType.getSqlTypeName());
    Preconditions.checkState(objects.length == rowType.getFieldCount(),
        "Objects counts and type counts must match " + objects.length + " vs " + rowType.getFieldCount());
    List<String> names = new ArrayList<>(rowType.getFieldNames());
    List<Object> values = new ArrayList<>(rowType.getFieldCount());
    for (int i = 0; i < objects.length; i++) {
      Object val = objects[i];
      if (val == null) {
        values.add(null);
        continue;
      }
      final RelDataType valueType = rowType.getFieldList().get(i).getType();
      values.add(convertToSamzaSqlType(val, valueType));
    }
    return new SamzaSqlRelRecord(names, values);
  }

  /**
   * Recursively converts a Primitive Java Object to valid Samza Rel Record field type.
   *
   * @param value input value to be converted
   * @param dataType value type as derived by Calcite
   * @return SamzaRelRecord or primitive SamzaRelRecord field.
   *
   */
  private static Object convertToSamzaSqlType(Object value, RelDataType dataType) {
    if (value == null) {
      return null;
    }
    switch (dataType.getSqlTypeName()) {
      case ROW:
        List<String> names = new ArrayList<>(dataType.getFieldNames());
        // Row Struct is represent as Object array in Calcite.
        Object[] row = (Object[]) value;
        List<Object> values = new ArrayList<>(row.length);
        for (int i = 0; i < row.length; i++) {
          values.add(convertToSamzaSqlType(row[i], dataType.getFieldList().get(i).getType()));
        }
        return new SamzaSqlRelRecord(names, values);
      case MAP:
        Map<Object, Object> objectMap = (Map<Object, Object>) value;
        Map<Object, Object> resultMap = new HashMap<>();
        final RelDataType valuesType = dataType.getValueType();
        objectMap.forEach((key, v) -> resultMap.put(key, convertToSamzaSqlType(v, valuesType)));
        return resultMap;
      case ARRAY:
        List<Object> objectList = (List<Object>) value;
        final RelDataType elementsType = dataType.getComponentType();
        return objectList.stream().map(e -> convertToSamzaSqlType(e, elementsType)).collect(Collectors.toList());
      case BOOLEAN:
      case BIGINT:
      case BINARY:
      case INTEGER:
      case TINYINT:
      case DOUBLE:
      case FLOAT:
      case REAL:
      case VARCHAR:
      case CHAR:
      case VARBINARY:
      case ANY:
      case OTHER:
        // today we treat everything else as Type Any or Other, this is not ideal.
        // this will change when adding timestamps support or more complex non java primitive types.
        // TODO in a better world we need to add type factory that can do the conversion between calcite and samza.
        return value;
      default:
        // As of today we treat everything else as type ANY
        throw new IllegalStateException("Unknown SQL type " + dataType.getSqlTypeName());
    }
  }

  /**
   * Converts the Samza Record to a Java Primitive Row format that's in convention with Calcite Enum operators.
   *
   * @param samzaSqlRelRecord input record.
   * @return row of Java Primitive conform to org.apache.calcite.adapter.enumerable.JavaRowFormat#ARRAY
   */
  public static Object[] convertToJavaRow(SamzaSqlRelRecord samzaSqlRelRecord) {
    if (samzaSqlRelRecord == null) {
      return null;
    }
    Object[] inputRow = new Object[samzaSqlRelRecord.getFieldValues().size()];
    for (int i = 0; i < inputRow.length; i++) {
      inputRow[i] = asPrimitiveJavaRow(samzaSqlRelRecord.getFieldValues().get(i));
    }
    return inputRow;
  }

  private static Object asPrimitiveJavaRow(Object inputObject) {
    if (inputObject == null) {
      return null;
    }
    if (inputObject instanceof SamzaSqlRelRecord) {
      return convertToJavaRow((SamzaSqlRelRecord) inputObject);
    }
    if (inputObject instanceof List) {
      return ((List) inputObject).stream().map(e -> asPrimitiveJavaRow(e)).collect(Collectors.toList());
    }
    if (inputObject instanceof Map) {
      Map<Object, Object> objectMap = new HashMap<>();
      ((Map<Object, Object>) inputObject).forEach((k, v) -> objectMap.put(k, asPrimitiveJavaRow(v)));
      return objectMap;
    }
    return inputObject;
  }

  /**
   * ProjectMapFunction implements MapFunction to map input SamzaSqlRelMessages, one at a time, to a new
   * SamzaSqlRelMessage which consists of the projected fields
   */
  private static class ProjectMapFunction implements MapFunction<SamzaSqlRelMessage, SamzaSqlRelMessage> {
    private transient Project project;
    private transient Expression expr;
    private transient TranslatorContext translatorContext;
    private transient MetricsRegistry metricsRegistry;
    private transient SamzaHistogram processingTime; // milli-seconds
    private transient Counter inputEvents;
    private transient Counter outputEvents;

    private final int queryId;
    private final int projectId;
    private final String logicalOpId;
    private Context context;

    ProjectMapFunction(int projectId, int queryId, String logicalOpId) {
      this.projectId = projectId;
      this.queryId = queryId;
      this.logicalOpId = logicalOpId;
    }

    /**
     * initializes the ProjectMapFunction before any message is processed
     * @param context the {@link Context} for this task
     */
    @Override
    public void init(Context context) {
      this.context = context;
      this.translatorContext =
          ((SamzaSqlApplicationContext) context.getApplicationTaskContext()).getTranslatorContexts().get(queryId);
      this.project = (Project) this.translatorContext.getRelNode(projectId);
      LOG.info("Compiling operator {} ", project.getDigest());
      this.expr = this.translatorContext.getExpressionCompiler().compile(project.getInputs(), project.getProjects());
      ContainerContext containerContext = context.getContainerContext();
      metricsRegistry = containerContext.getContainerMetricsRegistry();
      processingTime = new SamzaHistogram(metricsRegistry, logicalOpId, TranslatorConstants.PROCESSING_TIME_NAME);
      inputEvents = metricsRegistry.newCounter(logicalOpId, TranslatorConstants.INPUT_EVENTS_NAME);
      inputEvents.clear();
      outputEvents = metricsRegistry.newCounter(logicalOpId, TranslatorConstants.OUTPUT_EVENTS_NAME);
      outputEvents.clear();
    }

    /**
     * transforms the input message into the output message with projected fields
     * @param message  the input message to be transformed
     * @return the new SamzaSqlRelMessage message
     */
    @Override
    public SamzaSqlRelMessage apply(SamzaSqlRelMessage message) {
      long arrivalTime = System.nanoTime();
      RelDataType type = project.getRowType();
      Object[] output = new Object[type.getFieldCount()];
      Object[] inputRow = convertToJavaRow(message.getSamzaSqlRelRecord());
      SamzaSqlExecutionContext execContext = translatorContext.getExecutionContext();
      DataContext dataRootContext = translatorContext.getDataContext();
      try {
        expr.execute(execContext, context, dataRootContext, inputRow, output);
      } catch (Exception e) {
        String errMsg = String.format("Handling the following rel message ran into an error. %s", message);
        LOG.error(errMsg, e);
        throw new SamzaException(errMsg, e);
      }
      SamzaSqlRelRecord record = buildSamzaRelRecord(output, project.getRowType());
      updateMetrics(arrivalTime, System.nanoTime(), message.getSamzaSqlRelMsgMetadata().isNewInputMessage);
      return new SamzaSqlRelMessage(record, message.getSamzaSqlRelMsgMetadata());
    }

    /**
     * Updates the Diagnostics Metrics (processing time and number of events)
     * @param arrivalTime input message arrival time (= beging of processing in this operator)
     * @param outputTime output message output time (=end of processing in this operator)
     * @param isNewInputMessage whether the input Message is from new input message or not
     */
    private void updateMetrics(long arrivalTime, long outputTime, boolean isNewInputMessage) {
      if (isNewInputMessage) {
        inputEvents.inc();
      }
      outputEvents.inc();
      processingTime.update(outputTime - arrivalTime);
    }
  }

  private MessageStream<SamzaSqlRelMessage> translateFlatten(Integer flattenIndex,
      MessageStream<SamzaSqlRelMessage> inputStream) {
    return inputStream.flatMap(message -> {
      Object targetFlattenColumn = message.getSamzaSqlRelRecord().getFieldValues().get(flattenIndex);
      final List<SamzaSqlRelMessage> outMessages = new ArrayList<>();
      if (targetFlattenColumn != null && targetFlattenColumn instanceof List) {
        List<Object> objectList = (List<Object>) targetFlattenColumn;
        SamzaSqlRelMsgMetadata messageMetadata = message.getSamzaSqlRelMsgMetadata();
        SamzaSqlRelMsgMetadata newMetadata =
            new SamzaSqlRelMsgMetadata(messageMetadata.getEventTime(), messageMetadata.getArrivalTime(),
                messageMetadata.getScanTimeNanos(), messageMetadata.getScanTimeMillis());
        for (Object fieldValue : objectList) {
          List<Object> newValues = new ArrayList<>(message.getSamzaSqlRelRecord().getFieldValues());
          newValues.set(flattenIndex, fieldValue);
          outMessages.add(
              new SamzaSqlRelMessage(message.getSamzaSqlRelRecord().getFieldNames(), newValues, newMetadata));
          newMetadata = new SamzaSqlRelMsgMetadata(newMetadata.getEventTime(), newMetadata.getArrivalTime(),
              newMetadata.getScanTimeNanos(), newMetadata.getScanTimeMillis());
        }
      } else {
        message.getSamzaSqlRelMsgMetadata().isNewInputMessage = true;
        outMessages.add(message);
      }
      return outMessages;
    });
  }

  private boolean isFlatten(RexNode rexNode) {
    return rexNode instanceof RexCall && ((RexCall) rexNode).op instanceof SqlUserDefinedFunction
        && ((RexCall) rexNode).op.getName().equalsIgnoreCase("flatten");
  }

  void translate(final Project project, final String logicalOpId, final TranslatorContext context) {
    MessageStream<SamzaSqlRelMessage> messageStream = context.getMessageStream(project.getInput().getId());

    final int projectId = project.getId();

    MessageStream<SamzaSqlRelMessage> outputStream =
        messageStream.map(new ProjectMapFunction(projectId, queryId, logicalOpId));

    List<RexNode> projects = project.getProjects();
    List<Integer> flattenProjects = IntStream.range(0, projects.size())
        .filter(i -> this.isFlatten(projects.get(i)))
        .boxed()
        .collect(Collectors.toList());

    if (flattenProjects.size() > 0) {
      if (flattenProjects.size() > 1) {
        String msg = "Multiple flatten operators in a single query is not supported";
        LOG.error(msg);
        throw new SamzaException(msg);
      }
      outputStream = translateFlatten(flattenProjects.get(0), outputStream);
    }

    context.registerMessageStream(project.getId(), outputStream);
    context.registerRelNode(project.getId(), project);
  }
}
