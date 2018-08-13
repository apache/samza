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

package org.apache.samza.tools;

import com.google.common.base.Joiner;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.serializers.StringSerdeFactory;
import org.apache.samza.sql.avro.ConfigBasedAvroRelSchemaProviderFactory;
import org.apache.samza.sql.fn.FlattenUdf;
import org.apache.samza.sql.fn.RegexMatchUdf;
import org.apache.samza.sql.impl.ConfigBasedIOResolverFactory;
import org.apache.samza.sql.impl.ConfigBasedUdfResolver;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationRunner;
import org.apache.samza.sql.testutil.JsonUtil;
import org.apache.samza.sql.testutil.SqlFileParser;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;
import org.apache.samza.system.kafka.KafkaSystemFactory;
import org.apache.samza.tools.avro.AvroSchemaGenRelConverterFactory;
import org.apache.samza.tools.avro.AvroSerDeFactory;
import org.apache.samza.tools.json.JsonRelConverterFactory;
import org.apache.samza.tools.schemas.PageViewEvent;
import org.apache.samza.tools.schemas.ProfileChangeEvent;


public class SamzaSqlConsole {

  private static final String OPT_SHORT_SQL_FILE = "f";
  private static final String OPT_LONG_SQL_FILE = "file";
  private static final String OPT_ARG_SQL_FILE = "SQL_FILE";
  private static final String OPT_DESC_SQL_FILE = "Path to the SQL file to execute.";

  private static final String OPT_SHORT_SQL_STMT = "s";
  private static final String OPT_LONG_SQL_STMT = "sql";
  private static final String OPT_ARG_SQL_STMT = "SQL_STMT";
  private static final String OPT_DESC_SQL_STMT = "SQL statement to execute.";

  private static final String SAMZA_SYSTEM_KAFKA = "kafka";
  private static final String SAMZA_SYSTEM_LOG = "log";

  public static void main(String[] args) {
    Options options = new Options();
    options.addOption(
        CommandLineHelper.createOption(OPT_SHORT_SQL_FILE, OPT_LONG_SQL_FILE, OPT_ARG_SQL_FILE, false, OPT_DESC_SQL_FILE));
    options.addOption(
        CommandLineHelper.createOption(OPT_SHORT_SQL_STMT, OPT_LONG_SQL_STMT, OPT_ARG_SQL_STMT, false, OPT_DESC_SQL_STMT));

    CommandLineParser parser = new BasicParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, args);
      if (!cmd.hasOption(OPT_SHORT_SQL_STMT) && !cmd.hasOption(OPT_SHORT_SQL_FILE)) {
        throw new Exception(
            String.format("One of the (%s or %s) options needs to be set", OPT_SHORT_SQL_FILE, OPT_SHORT_SQL_STMT));
      }
    } catch (Exception e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(String.format("Error: %s%nsamza-sql-console.sh", e.getMessage()), options);
      return;
    }

    List<String> sqlStmts;

    if (cmd.hasOption(OPT_SHORT_SQL_FILE)) {
      String sqlFile = cmd.getOptionValue(OPT_SHORT_SQL_FILE);
      sqlStmts = SqlFileParser.parseSqlFile(sqlFile);
    } else {
      String sql = cmd.getOptionValue(OPT_SHORT_SQL_STMT);
      System.out.println("Executing sql " + sql);
      sqlStmts = Collections.singletonList(sql);
    }

    executeSql(sqlStmts);
  }

  public static void executeSql(List<String> sqlStmts) {
    Map<String, String> staticConfigs = fetchSamzaSqlConfig();
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SQL_STMTS_JSON, JsonUtil.toJson(sqlStmts));
    SamzaSqlApplicationRunner appRunnable = new SamzaSqlApplicationRunner(true, new MapConfig(staticConfigs));
    appRunnable.runAndWaitForFinish();
  }

  public static Map<String, String> fetchSamzaSqlConfig() {
    HashMap<String, String> staticConfigs = new HashMap<>();

    staticConfigs.put(JobConfig.JOB_NAME(), "sql-job");
    staticConfigs.put(JobConfig.PROCESSOR_ID(), "1");
    staticConfigs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    staticConfigs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_IO_RESOLVER, "config");
    String configIOResolverDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SOURCE_RESOLVER_DOMAIN, "config");
    staticConfigs.put(configIOResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        ConfigBasedIOResolverFactory.class.getName());

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_UDF_RESOLVER, "config");
    String configUdfResolverDomain = String.format(SamzaSqlApplicationConfig.CFG_FMT_UDF_RESOLVER_DOMAIN, "config");
    staticConfigs.put(configUdfResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        ConfigBasedUdfResolver.class.getName());
    staticConfigs.put(configUdfResolverDomain + ConfigBasedUdfResolver.CFG_UDF_CLASSES,
        Joiner.on(",").join(RegexMatchUdf.class.getName(), FlattenUdf.class.getName()));

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
    staticConfigs.put(logSystemConfigPrefix + "samza.factory", ConsoleLoggingSystemFactory.class.getName());
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
        ConfigBasedAvroRelSchemaProviderFactory.class.getName());

    staticConfigs.put(
        configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
            "kafka", "PageViewStream"), PageViewEvent.SCHEMA$.toString());

    staticConfigs.put(
        configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
            "kafka", "ProfileChangeStream"), ProfileChangeEvent.SCHEMA$.toString());

    return staticConfigs;
  }
}
