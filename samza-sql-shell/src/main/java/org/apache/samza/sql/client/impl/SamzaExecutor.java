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

package org.apache.samza.sql.client.impl;

import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.I0Itec.zkclient.exception.ZkTimeoutException;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.samza.SamzaException;
import org.apache.samza.config.*;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.serializers.StringSerdeFactory;
import org.apache.samza.sql.client.exceptions.ExecutorException;
import org.apache.samza.sql.client.interfaces.ExecutionStatus;
import org.apache.samza.sql.client.interfaces.*;
import org.apache.samza.sql.client.util.Pair;
import org.apache.samza.sql.client.util.RandomAccessQueue;
import org.apache.samza.sql.dsl.SamzaSqlDslConverter;
import org.apache.samza.sql.dsl.SamzaSqlDslConverterFactory;
import org.apache.samza.sql.impl.ConfigBasedIOResolverFactory;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.RelSchemaProviderFactory;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.interfaces.SqlIOResolver;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.schema.SamzaSqlFieldType;
import org.apache.samza.sql.schema.SqlFieldSchema;
import org.apache.samza.sql.schema.SqlSchema;
import org.apache.samza.sql.util.JsonUtil;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.kafka.KafkaSystemFactory;
import org.apache.samza.tools.avro.AvroSchemaGenRelConverterFactory;
import org.apache.samza.tools.avro.AvroSerDeFactory;
import org.apache.samza.tools.json.JsonRelConverterFactory;
import org.apache.samza.tools.schemas.ProfileChangeEvent;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


/**
 * Samza implementation of Executor for Samza SQL Shell.
 */
public class SamzaExecutor implements SqlExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(SamzaExecutor.class);

  private static final String SAMZA_SYSTEM_LOG = "log";
  private static final String SAMZA_SYSTEM_KAFKA = "kafka";
  private static final String SAMZA_SQL_OUTPUT = "samza.sql.output";
  private static final String SAMZA_SQL_SYSTEM_KAFKA_ADDRESS = "samza.sql.system.kafka.address";
  private static final String DEFAULT_SERVER_ADDRESS = "localhost:2181";

  // The maximum number of rows of data we keep when user pauses the display view and data accumulates.
  private static final int RANDOM_ACCESS_QUEUE_CAPACITY = 5000;
  private static final int DEFAULT_ZOOKEEPER_CLIENT_TIMEOUT = 20000;

  private static RandomAccessQueue<OutgoingMessageEnvelope> outputData =
          new RandomAccessQueue<>(OutgoingMessageEnvelope.class, RANDOM_ACCESS_QUEUE_CAPACITY);
  private static AtomicInteger execIdSeq = new AtomicInteger(0);
  private Map<Integer, SamzaSqlApplicationRunner> executions = new HashMap<>();
  private SamzaExecutorEnvironmentVariableHandler environmentVariableHandler =
          new SamzaExecutorEnvironmentVariableHandler();

  // -- implementation of SqlExecutor ------------------------------------------

  @Override
  public void start(ExecutionContext context) {
  }

  @Override
  public void stop(ExecutionContext context) throws ExecutorException {
    Iterator<Integer> iter = executions.keySet().iterator();
    while (iter.hasNext()) {
      stopExecution(context, iter.next());
      iter.remove();
    }
    outputData.clear();
  }

  @Override
  public EnvironmentVariableHandler getEnvironmentVariableHandler() {
    return environmentVariableHandler;
  }

  @Override
  public List<String> listTables(ExecutionContext context) throws ExecutorException {
    String address = environmentVariableHandler.getEnvironmentVariable(SAMZA_SQL_SYSTEM_KAFKA_ADDRESS);
    if (address == null || address.isEmpty()) {
      address = DEFAULT_SERVER_ADDRESS;
    }
    try {
      ZkUtils zkUtils = new ZkUtils(new ZkClient(address, DEFAULT_ZOOKEEPER_CLIENT_TIMEOUT),
          new ZkConnection(address), false);
      return JavaConversions.seqAsJavaList(zkUtils.getAllTopics())
        .stream()
        .map(x -> SAMZA_SYSTEM_KAFKA + "." + x)
        .collect(Collectors.toList());
    } catch (ZkTimeoutException ex) {
      throw new ExecutorException(ex);
    }
  }

  @Override
  public SqlSchema getTableSchema(ExecutionContext context, String tableName) throws ExecutorException {
    /*
     *  currently Shell works only for systems that has Avro schemas
     */
    int execId = execIdSeq.incrementAndGet();
    Map<String, String> staticConfigs = fetchSamzaSqlConfig(execId);
    Config samzaSqlConfig = new MapConfig(staticConfigs);
    SqlSchema sqlSchema;
    try {
      SqlIOResolver ioResolver = SamzaSqlApplicationConfig.createIOResolver(samzaSqlConfig);
      SqlIOConfig sourceInfo = ioResolver.fetchSourceInfo(tableName);
      RelSchemaProvider schemaProvider =
        SamzaSqlApplicationConfig.initializePlugin("RelSchemaProvider", sourceInfo.getRelSchemaProviderName(),
          samzaSqlConfig, SamzaSqlApplicationConfig.CFG_FMT_REL_SCHEMA_PROVIDER_DOMAIN,
          (o, c) -> ((RelSchemaProviderFactory) o).create(sourceInfo.getSystemStream(), c));
      sqlSchema =  schemaProvider.getSqlSchema();
    } catch (SamzaException ex) {
      throw new ExecutorException(ex);
    }
    return sqlSchema;
  }

  @Override
  public QueryResult executeQuery(ExecutionContext context, String statement) throws ExecutorException {
    outputData.clear();

    int execId = execIdSeq.incrementAndGet();
    Map<String, String> staticConfigs = fetchSamzaSqlConfig(execId);
    List<String> sqlStmts = formatSqlStmts(Collections.singletonList(statement));
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));

    SamzaSqlApplicationRunner runner;
    try {
      runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
      runner.run(null);
    } catch (SamzaException ex) {
      throw new ExecutorException(ex);
    }
    executions.put(execId, runner);
    LOG.debug("Executing sql. Id ", execId);

    SqlSchema resultSchema = null;
    // TODO: after fixing the TODO in generateResultSchema function, we can uncomment the following piece of code.
    // resultSchema = generateResultSchema(new MapConfig(staticConfigs));
    return new QueryResult(execId, resultSchema);
  }

  @Override
  public int getRowCount() {
    return outputData.getSize();
  }

  @Override
  public List<String[]> retrieveQueryResult(ExecutionContext context, int startRow, int endRow) {
    List<String[]> results = new ArrayList<>();
    for (OutgoingMessageEnvelope row : outputData.get(startRow, endRow)) {
      results.add(getFormattedRow(row));
    }
    return results;
  }

  @Override
  public List<String[]> consumeQueryResult(ExecutionContext context, int startRow, int endRow) {
    List<String[]> results = new ArrayList<>();
    for (OutgoingMessageEnvelope row : outputData.consume(startRow, endRow)) {
      results.add(getFormattedRow(row));
    }
    return results;
  }

  @Override
  public NonQueryResult executeNonQuery(ExecutionContext context, File sqlFile) throws ExecutorException {
    LOG.info("Sql file path: " + sqlFile.getPath());
    List<String> executedStmts;
    try {
      executedStmts = Files.lines(Paths.get(sqlFile.getPath())).collect(Collectors.toList());
    } catch (IOException e) {
      throw new ExecutorException(e);
    }
    LOG.info("Sql statements in Sql file: " + executedStmts.toString());

    List<String> submittedStmts = new ArrayList<>();
    List<String> nonSubmittedStmts = new ArrayList<>();
    validateExecutedStmts(executedStmts, submittedStmts, nonSubmittedStmts);
    if (submittedStmts.isEmpty()) {
      throw new ExecutorException("Nothing to execute. Note: SELECT statements are ignored.");
    }
    NonQueryResult result = executeNonQuery(context, submittedStmts);
    return new NonQueryResult(result.getExecutionId(), submittedStmts, nonSubmittedStmts);
  }

  @Override
  public NonQueryResult executeNonQuery(ExecutionContext context, List<String> statement) throws ExecutorException {
    int execId = execIdSeq.incrementAndGet();
    Map<String, String> staticConfigs = fetchSamzaSqlConfig(execId);
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(formatSqlStmts(statement)));

    SamzaSqlApplicationRunner runner;
    try {
      runner = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
      runner.run(null);
    } catch (SamzaException ex) {
      throw new ExecutorException(ex);
    }
    executions.put(execId, runner);
    LOG.debug("Executing sql. Id ", execId);

    return new NonQueryResult(execId);
  }

  @Override
  public void stopExecution(ExecutionContext context, int exeId) throws ExecutorException {
    SamzaSqlApplicationRunner runner = executions.get(exeId);
    if (runner != null) {
      LOG.debug("Stopping execution ", exeId);
      try {
        runner.kill();
      } catch (SamzaException ex) {
        throw new ExecutorException(ex);
      }
    } else {
      throw new ExecutorException("Trying to stop a non-existing SQL execution " + exeId);
    }
  }

  @Override
  public void removeExecution(ExecutionContext context, int exeId) throws ExecutorException {
    SamzaSqlApplicationRunner runner = executions.get(exeId);
    if (runner != null) {
      if (runner.status().getStatusCode().equals(ApplicationStatus.StatusCode.Running)) {
        throw new ExecutorException("Trying to remove an ongoing execution " + exeId);
      }
      executions.remove(exeId);
    } else {
      throw new ExecutorException("Trying to remove a non-existing SQL execution " + exeId);
    }
  }

  @Override
  public ExecutionStatus queryExecutionStatus(int execId) throws ExecutorException {
    SamzaSqlApplicationRunner runner = executions.get(execId);
    if (runner == null) {
      throw new ExecutorException("Execution " + execId + " does not exist.");
    }
    return queryExecutionStatus(runner);
  }

  @Override
  public List<SqlFunction> listFunctions(ExecutionContext context) {
    /*
     * TODO: currently the Shell only shows some UDFs supported by Samza internally. We may need to require UDFs
     *       to provide a function of getting their "SamzaSqlUdfDisplayInfo", then we can get the UDF information from
     *       SamzaSqlApplicationConfig.udfResolver(or SamzaSqlApplicationConfig.udfMetadata) instead of registering
     *       UDFs one by one as below.
     */
    List<SqlFunction> udfs = new ArrayList<>();
    udfs.add(new SamzaSqlUdfDisplayInfo("RegexMatch", "Matches the string to the regex",
            Arrays.asList(SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.STRING, false, false),
                SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.STRING, false, false)),
        SqlFieldSchema.createPrimitiveSchema(SamzaSqlFieldType.BOOLEAN, false, false)));

    return udfs;
  }

  @Override
  public String getVersion() {
    return this.getClass().getPackage().getImplementationVersion();
  }

  static void saveOutputMessage(OutgoingMessageEnvelope messageEnvelope) {
    outputData.add(messageEnvelope);
  }

  private boolean processEnvironmentVariable(String name, String value) {
    return true;
  }

  Map<String, String> fetchSamzaSqlConfig(int execId) {
    HashMap<String, String> staticConfigs = new HashMap<>();

    staticConfigs.put(JobConfig.JOB_NAME, "sql-job-" + execId);
    staticConfigs.put(JobConfig.PROCESSOR_ID, String.valueOf(execId));
    staticConfigs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    staticConfigs.put(TaskConfig.GROUPER_FACTORY, SingleContainerGrouperFactory.class.getName());

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_IO_RESOLVER, "config");
    String configIOResolverDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SOURCE_RESOLVER_DOMAIN, "config");
    staticConfigs.put(configIOResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        ConfigBasedIOResolverFactory.class.getName());

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_UDF_RESOLVER, "config");

    staticConfigs.put("serializers.registry.string.class", StringSerdeFactory.class.getName());
    staticConfigs.put("serializers.registry.avro.class", AvroSerDeFactory.class.getName());
    staticConfigs.put(AvroSerDeFactory.CFG_AVRO_SCHEMA, ProfileChangeEvent.SCHEMA$.toString());

    String kafkaSystemConfigPrefix =
        String.format(ConfigBasedIOResolverFactory.CFG_FMT_SAMZA_PREFIX, SAMZA_SYSTEM_KAFKA);
    String avroSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", SAMZA_SYSTEM_KAFKA);
    staticConfigs.put(kafkaSystemConfigPrefix + "samza.factory", KafkaSystemFactory.class.getName());
    staticConfigs.put(kafkaSystemConfigPrefix + "samza.key.serde", "string");
    staticConfigs.put(kafkaSystemConfigPrefix + "samza.msg.serde", "avro");
    staticConfigs.put(kafkaSystemConfigPrefix + "consumer.zookeeper.connect", "localhost:2181");
    staticConfigs.put(kafkaSystemConfigPrefix + "producer.bootstrap.servers", "localhost:9092");

    staticConfigs.put(kafkaSystemConfigPrefix + "samza.offset.reset", "true");
    staticConfigs.put(kafkaSystemConfigPrefix + "samza.offset.default", "oldest");

    staticConfigs.put(avroSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER, "avro");
    staticConfigs.put(avroSamzaSqlConfigPrefix + SqlIOConfig.CFG_REL_SCHEMA_PROVIDER, "config");

    String logSystemConfigPrefix =
        String.format(ConfigBasedIOResolverFactory.CFG_FMT_SAMZA_PREFIX, SAMZA_SYSTEM_LOG);
    String logSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", SAMZA_SYSTEM_LOG);
    staticConfigs.put(logSystemConfigPrefix + "samza.factory", CliLoggingSystemFactory.class.getName());
    staticConfigs.put(logSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER, "json");
    staticConfigs.put(logSamzaSqlConfigPrefix + SqlIOConfig.CFG_REL_SCHEMA_PROVIDER, "config");

    String avroSamzaToRelMsgConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, "avro");

    staticConfigs.put(avroSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        AvroSchemaGenRelConverterFactory.class.getName());

    String jsonSamzaToRelMsgConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, "json");

    staticConfigs.put(jsonSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        JsonRelConverterFactory.class.getName());

    String configAvroRelSchemaProviderDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_REL_SCHEMA_PROVIDER_DOMAIN, "config");
    staticConfigs.put(configAvroRelSchemaProviderDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        FileSystemAvroRelSchemaProviderFactory.class.getName());

    staticConfigs.put(
        configAvroRelSchemaProviderDomain + FileSystemAvroRelSchemaProviderFactory.CFG_SCHEMA_DIR,
        "/tmp/schemas/");

    List<Pair<String, String>> allEnvironmentVariables = environmentVariableHandler.getAllEnvironmentVariables();
    for (Pair<String, String> p : allEnvironmentVariables) {
      staticConfigs.put(p.getL(), p.getR());
    }

    return staticConfigs;
  }

  private List<String> formatSqlStmts(List<String> statements) {
    return statements.stream().map(sql -> {
      if (!sql.toLowerCase().startsWith("insert")) {
        String formattedSql = String.format("insert into log.outputStream %s", sql);
        LOG.debug("Sql formatted. ", sql, formattedSql);
        return formattedSql;
      } else {
        return sql;
      }
    }).collect(Collectors.toList());
  }

  private void validateExecutedStmts(List<String> statements, List<String> submittedStmts,
                                     List<String> nonSubmittedStmts) {
    for (String sql : statements) {
      if (sql.isEmpty()) {
        continue;
      }
      if (!sql.toLowerCase().startsWith("insert")) {
        nonSubmittedStmts.add(sql);
      } else {
        submittedStmts.add(sql);
      }
    }
  }

  SqlSchema generateResultSchema(Config config) {
    SamzaSqlDslConverter converter = (SamzaSqlDslConverter) new SamzaSqlDslConverterFactory().create(config);
    RelRoot relRoot = converter.convertDsl("").iterator().next();

    List<String> colNames = new ArrayList<>();
    List<String> colTypeNames = new ArrayList<>();
    for (RelDataTypeField dataTypeField : relRoot.validatedRowType.getFieldList()) {
      colNames.add(dataTypeField.getName());
      colTypeNames.add(dataTypeField.getType().toString());
    }

    // TODO: Need to find a way to convert the relational to SQL Schema. After fixing this TODO, please resolve the TODOs
    // in QueryResult class and executeQuery().
    return new SqlSchema(colNames, Collections.emptyList());
  }

  private String[] getFormattedRow(OutgoingMessageEnvelope row) {
    String[] formattedRow = new String[1];
    String outputFormat = environmentVariableHandler.getEnvironmentVariable(SAMZA_SQL_OUTPUT);
    if (outputFormat == null || !outputFormat.equalsIgnoreCase(MessageFormat.PRETTY.toString())) {
      formattedRow[0] = getCompressedFormat(row);
    } else {
      formattedRow[0] = getPrettyFormat(row);
    }
    return formattedRow;
  }

  private ExecutionStatus queryExecutionStatus(SamzaSqlApplicationRunner runner) throws ExecutorException {
    ApplicationStatus.StatusCode code = runner.status().getStatusCode();
    switch (code) {
      case New:
        return ExecutionStatus.New;
      case Running:
        return ExecutionStatus.Running;
      case SuccessfulFinish:
        return ExecutionStatus.SuccessfulFinish;
      case UnsuccessfulFinish:
        return ExecutionStatus.UnsuccessfulFinish;
    }
    throw new ExecutorException("Unsupported status code: " + code);
  }

  private String getPrettyFormat(OutgoingMessageEnvelope envelope) {
    String value = new String((byte[]) envelope.getMessage());
    ObjectMapper mapper = new ObjectMapper();
    String formattedValue;
    try {
      Object json = mapper.readValue(value, Object.class);
      formattedValue = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
    } catch (IOException ex) {
      formattedValue = value;
      LOG.error("getPrettyFormat failed with exception while formatting json ", ex);
    }
    return formattedValue;
  }

  private String getCompressedFormat(OutgoingMessageEnvelope envelope) {
    return new String((byte[]) envelope.getMessage());
  }

  private enum MessageFormat {
    PRETTY,
    COMPACT
  }

  class SamzaExecutorEnvironmentVariableHandler extends EnvironmentVariableHandlerImpl {
    SamzaExecutorEnvironmentVariableHandler() {
    }

    protected EnvironmentVariableSpecs initializeEnvironmentVariableSpecs() {
      HashMap<String, EnvironmentVariableSpecs.Spec> specMap = new HashMap<>();
      specMap.put("samza.sql.output",
              new EnvironmentVariableSpecs.Spec(new String[]{"pretty", "compact"}, "compact"));
      return new EnvironmentVariableSpecs(specMap);
    }

    protected boolean processEnvironmentVariable(String name, String value) {
      return SamzaExecutor.this.processEnvironmentVariable(name, value);
    }

    protected boolean isAcceptUnknowName() {
      return true;
    }
  }
}
