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

package org.apache.samza.sql.testutil;

import com.google.common.base.Joiner;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.sql.avro.AvroRelConverterFactory;
import org.apache.samza.sql.avro.ConfigBasedAvroRelSchemaProviderFactory;
import org.apache.samza.sql.avro.schemas.ComplexRecord;
import org.apache.samza.sql.avro.schemas.SimpleRecord;
import org.apache.samza.sql.fn.FlattenUdf;
import org.apache.samza.sql.fn.RegexMatchUdf;
import org.apache.samza.sql.impl.ConfigBasedSourceResolverFactory;
import org.apache.samza.sql.impl.ConfigBasedUdfResolver;
import org.apache.samza.sql.interfaces.SqlSystemStreamConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.system.TestAvroSystemFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;


/**
 * Utility to hookup the configs needed to run the Samza Sql application.
 */
public class SamzaSqlTestConfig {

  public static final String SAMZA_SYSTEM_TEST_AVRO = "testavro";
  public static final String SAMZA_SYSTEM_TEST_DB = "testDb";

  public static Map<String, String> fetchStaticConfigsWithFactories(int numberOfMessages) {
    HashMap<String, String> staticConfigs = new HashMap<>();

    staticConfigs.put(JobConfig.JOB_NAME(), "sql-job");
    staticConfigs.put(JobConfig.PROCESSOR_ID(), "1");
    staticConfigs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    staticConfigs.put(TaskConfig.GROUPER_FACTORY(), SingleContainerGrouperFactory.class.getName());

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_SOURCE_RESOLVER, "config");
    String configSourceResolverDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SOURCE_RESOLVER_DOMAIN, "config");
    staticConfigs.put(configSourceResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        TestSourceResolverFactory.class.getName());

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_UDF_RESOLVER, "config");
    String configUdfResolverDomain = String.format(SamzaSqlApplicationConfig.CFG_FMT_UDF_RESOLVER_DOMAIN, "config");
    staticConfigs.put(configUdfResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        ConfigBasedUdfResolver.class.getName());
    staticConfigs.put(configUdfResolverDomain + ConfigBasedUdfResolver.CFG_UDF_CLASSES, Joiner.on(",")
        .join(MyTestUdf.class.getName(), RegexMatchUdf.class.getName(), FlattenUdf.class.getName(),
            MyTestArrayUdf.class.getName()));

    String avroSystemConfigPrefix =
        String.format(ConfigBasedSourceResolverFactory.CFG_FMT_SAMZA_PREFIX, SAMZA_SYSTEM_TEST_AVRO);
    String avroSamzaSqlConfigPrefix = configSourceResolverDomain + String.format("%s.", SAMZA_SYSTEM_TEST_AVRO);
    staticConfigs.put(avroSystemConfigPrefix + "samza.factory", TestAvroSystemFactory.class.getName());
    staticConfigs.put(avroSystemConfigPrefix + TestAvroSystemFactory.CFG_NUM_MESSAGES,
        String.valueOf(numberOfMessages));
    staticConfigs.put(avroSamzaSqlConfigPrefix + SqlSystemStreamConfig.CFG_SAMZA_REL_CONVERTER, "avro");
    staticConfigs.put(avroSamzaSqlConfigPrefix + SqlSystemStreamConfig.CFG_REL_SCHEMA_PROVIDER, "config");

    String testDbSamzaSqlConfigPrefix = configSourceResolverDomain + String.format("%s.", SAMZA_SYSTEM_TEST_DB);
    staticConfigs.put(testDbSamzaSqlConfigPrefix + SqlSystemStreamConfig.CFG_SAMZA_REL_CONVERTER, "avro");
    staticConfigs.put(testDbSamzaSqlConfigPrefix + SqlSystemStreamConfig.CFG_REL_SCHEMA_PROVIDER, "config");

    String avroSamzaToRelMsgConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, "avro");
    staticConfigs.put(avroSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        AvroRelConverterFactory.class.getName());

    String testDbSamzaToRelMsgConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, TestSourceResolverFactory.TEST_DB_SYSTEM);
    staticConfigs.put(testDbSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        AvroRelConverterFactory.class.getName());

    String configAvroRelSchemaProviderDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_REL_SCHEMA_PROVIDER_DOMAIN, "config");
    staticConfigs.put(configAvroRelSchemaProviderDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        ConfigBasedAvroRelSchemaProviderFactory.class.getName());

    staticConfigs.put(
        configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
            "testavro", "SIMPLE1"), SimpleRecord.SCHEMA$.toString());

    staticConfigs.put(
        configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
            "testavro", "outputTopic"), ComplexRecord.SCHEMA$.toString());

    staticConfigs.put(
        configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
            "testavro", "COMPLEX1"), ComplexRecord.SCHEMA$.toString());

    staticConfigs.put(
        configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
            TestSourceResolverFactory.TEST_DB_SYSTEM, "testTable"), SimpleRecord.SCHEMA$.toString());

    return staticConfigs;
  }
}
