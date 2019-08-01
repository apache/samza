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

package org.apache.samza.sql.util;

import com.google.common.base.Joiner;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.config.JobConfig;
import org.apache.samza.config.JobCoordinatorConfig;
import org.apache.samza.config.TaskConfig;
import org.apache.samza.container.grouper.task.SingleContainerGrouperFactory;
import org.apache.samza.sql.avro.AvroRelConverterFactory;
import org.apache.samza.sql.avro.ConfigBasedAvroRelSchemaProviderFactory;
import org.apache.samza.sql.avro.schemas.Company;
import org.apache.samza.sql.avro.schemas.ComplexRecord;
import org.apache.samza.sql.avro.schemas.EnrichedPageView;
import org.apache.samza.sql.avro.schemas.PageView;
import org.apache.samza.sql.avro.schemas.PageViewCount;
import org.apache.samza.sql.avro.schemas.Profile;
import org.apache.samza.sql.avro.schemas.SimpleRecord;
import org.apache.samza.sql.fn.BuildOutputRecordUdf;
import org.apache.samza.sql.fn.FlattenUdf;
import org.apache.samza.sql.fn.GetNestedFieldUdf;
import org.apache.samza.sql.fn.RegexMatchUdf;
import org.apache.samza.sql.impl.ConfigBasedIOResolverFactory;
import org.apache.samza.sql.impl.ConfigBasedUdfResolver;
import org.apache.samza.sql.interfaces.SqlIOConfig;
import org.apache.samza.sql.runner.SamzaSqlApplicationConfig;
import org.apache.samza.sql.system.TestAvroSystemFactory;
import org.apache.samza.standalone.PassthroughJobCoordinatorFactory;

import static org.apache.samza.sql.util.RemoteStoreIOResolverTestFactory.TEST_REMOTE_STORE_SYSTEM;


/**
 * Utility to hookup the configs needed to run the Samza Sql application.
 */
public class SamzaSqlTestConfig {

  public static final String SAMZA_SYSTEM_TEST_AVRO = "testavro";
  public static final String SAMZA_SYSTEM_TEST_AVRO2 = "testavro2";
  public static final String SAMZA_SYSTEM_TEST_DB = "testDb";
  public static final String SQL_JOB = "sql-job";
  public static final String SQL_JOB_PROCESSOR_ID = "1";

  public static Map<String, String> fetchStaticConfigsWithFactories(int numberOfMessages) {
    return fetchStaticConfigsWithFactories(new HashMap<>(), numberOfMessages, false);
  }

  public static Map<String, String> fetchStaticConfigsWithFactories(Map<String, String> props, int numberOfMessages) {
    return fetchStaticConfigsWithFactories(props, numberOfMessages, false);
  }

  public static Map<String, String> fetchStaticConfigsWithFactories(Map<String, String> props, int numberOfMessages,
      boolean includeNullForeignKeys) {
    return fetchStaticConfigsWithFactories(props, numberOfMessages, includeNullForeignKeys, false, 0);
  }

  public static Map<String, String> fetchStaticConfigsWithFactories(Map<String, String> props, int numberOfMessages,
      boolean includeNullForeignKeys, boolean includeNullSimpleRecords) {
    return fetchStaticConfigsWithFactories(props, numberOfMessages, includeNullForeignKeys, includeNullSimpleRecords, 0);
  }

  public static Map<String, String> fetchStaticConfigsWithFactories(Map<String, String> props, int numberOfMessages,
      boolean includeNullForeignKeys, boolean includeNullSimpleRecords, long windowDurationMs) {
    HashMap<String, String> staticConfigs = new HashMap<>();

    staticConfigs.put(JobConfig.JOB_NAME(), SQL_JOB);
    staticConfigs.put(JobConfig.PROCESSOR_ID(), SQL_JOB_PROCESSOR_ID);
    staticConfigs.put(JobCoordinatorConfig.JOB_COORDINATOR_FACTORY, PassthroughJobCoordinatorFactory.class.getName());
    staticConfigs.put(TaskConfig.GROUPER_FACTORY, SingleContainerGrouperFactory.class.getName());

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_IO_RESOLVER, "config");
    String configIOResolverDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SOURCE_RESOLVER_DOMAIN, "config");
    staticConfigs.put(configIOResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        RemoteStoreIOResolverTestFactory.class.getName());

    staticConfigs.put(SamzaSqlApplicationConfig.CFG_UDF_RESOLVER, "config");
    String configUdfResolverDomain = String.format(SamzaSqlApplicationConfig.CFG_FMT_UDF_RESOLVER_DOMAIN, "config");
    staticConfigs.put(configUdfResolverDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        ConfigBasedUdfResolver.class.getName());
    staticConfigs.put(configUdfResolverDomain + ConfigBasedUdfResolver.CFG_UDF_CLASSES, Joiner.on(",")
        .join(MyTestUdf.class.getName(), RegexMatchUdf.class.getName(), FlattenUdf.class.getName(),
            MyTestArrayUdf.class.getName(), BuildOutputRecordUdf.class.getName(), MyTestPolyUdf.class.getName(),
            MyTestObjUdf.class.getName(), GetNestedFieldUdf.class.getName()));

    String avroSystemConfigPrefix =
        String.format(ConfigBasedIOResolverFactory.CFG_FMT_SAMZA_PREFIX, SAMZA_SYSTEM_TEST_AVRO);
    String avroSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", SAMZA_SYSTEM_TEST_AVRO);
    staticConfigs.put(avroSystemConfigPrefix + "samza.factory", TestAvroSystemFactory.class.getName());
    staticConfigs.put(avroSystemConfigPrefix + TestAvroSystemFactory.CFG_NUM_MESSAGES,
        String.valueOf(numberOfMessages));
    staticConfigs.put(avroSystemConfigPrefix + TestAvroSystemFactory.CFG_INCLUDE_NULL_FOREIGN_KEYS,
        includeNullForeignKeys ? "true" : "false");
    staticConfigs.put(avroSystemConfigPrefix + TestAvroSystemFactory.CFG_INCLUDE_NULL_SIMPLE_RECORDS,
        includeNullSimpleRecords ? "true" : "false");
    staticConfigs.put(avroSystemConfigPrefix + TestAvroSystemFactory.CFG_SLEEP_BETWEEN_POLLS_MS,
        String.valueOf(windowDurationMs / 2));
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_GROUPBY_WINDOW_DURATION_MS, String.valueOf(windowDurationMs));
    staticConfigs.put(avroSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER, "avro");
    staticConfigs.put(avroSamzaSqlConfigPrefix + SqlIOConfig.CFG_REL_SCHEMA_PROVIDER, "config");

    String testRemoteStoreSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", TEST_REMOTE_STORE_SYSTEM);
    staticConfigs.put(testRemoteStoreSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER, "avro");
    staticConfigs.put(testRemoteStoreSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_TABLE_KEY_CONVERTER, "sample");
    staticConfigs.put(testRemoteStoreSamzaSqlConfigPrefix + SqlIOConfig.CFG_REL_SCHEMA_PROVIDER, "config");

    String avro2SystemConfigPrefix =
            String.format(ConfigBasedIOResolverFactory.CFG_FMT_SAMZA_PREFIX, SAMZA_SYSTEM_TEST_AVRO2);
    String avro2SamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", SAMZA_SYSTEM_TEST_AVRO2);
    staticConfigs.put(avro2SystemConfigPrefix + "samza.factory", TestAvroSystemFactory.class.getName());
    staticConfigs.put(avro2SystemConfigPrefix + TestAvroSystemFactory.CFG_NUM_MESSAGES,
            String.valueOf(numberOfMessages));
    staticConfigs.put(avro2SystemConfigPrefix + TestAvroSystemFactory.CFG_INCLUDE_NULL_FOREIGN_KEYS,
            includeNullForeignKeys ? "true" : "false");
    staticConfigs.put(avro2SystemConfigPrefix + TestAvroSystemFactory.CFG_SLEEP_BETWEEN_POLLS_MS,
            String.valueOf(windowDurationMs / 2));
    staticConfigs.put(SamzaSqlApplicationConfig.CFG_GROUPBY_WINDOW_DURATION_MS, String.valueOf(windowDurationMs));
    staticConfigs.put(avro2SamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER, "avro");
    staticConfigs.put(avro2SamzaSqlConfigPrefix + SqlIOConfig.CFG_REL_SCHEMA_PROVIDER, "config");

    String testDbSamzaSqlConfigPrefix = configIOResolverDomain + String.format("%s.", SAMZA_SYSTEM_TEST_DB);
    staticConfigs.put(testDbSamzaSqlConfigPrefix + SqlIOConfig.CFG_SAMZA_REL_CONVERTER, "avro");
    staticConfigs.put(testDbSamzaSqlConfigPrefix + SqlIOConfig.CFG_REL_SCHEMA_PROVIDER, "config");

    String avroSamzaToRelMsgConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, "avro");
    staticConfigs.put(avroSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        AvroRelConverterFactory.class.getName());

    String testRemoteStoreSamzaToRelMsgConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, TEST_REMOTE_STORE_SYSTEM);
    staticConfigs.put(testRemoteStoreSamzaToRelMsgConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        AvroRelConverterFactory.class.getName());

    String testRemoteStoreSamzaRelTableKeyConverterDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_SAMZA_REL_TABLE_KEY_CONVERTER_DOMAIN, "sample");
    staticConfigs.put(testRemoteStoreSamzaRelTableKeyConverterDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        SampleRelTableKeyConverterFactory.class.getName());

    String configAvroRelSchemaProviderDomain =
        String.format(SamzaSqlApplicationConfig.CFG_FMT_REL_SCHEMA_PROVIDER_DOMAIN, "config");
    staticConfigs.put(configAvroRelSchemaProviderDomain + SamzaSqlApplicationConfig.CFG_FACTORY,
        ConfigBasedAvroRelSchemaProviderFactory.class.getName());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "SIMPLE1"), SimpleRecord.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro2", "SIMPLE1"), SimpleRecord.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "SIMPLE2"), SimpleRecord.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "SIMPLE3"), SimpleRecord.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "simpleOutputTopic"), SimpleRecord.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "outputTopic"), ComplexRecord.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "COMPLEX1"), ComplexRecord.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "Profile"), ComplexRecord.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "PROFILE"), Profile.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "PROFILE1"), Profile.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "PAGEVIEW"), PageView.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "COMPANY"), Company.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "enrichedPageViewTopic"), EnrichedPageView.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
        "testavro", "pageViewCountTopic"), PageViewCount.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
            TEST_REMOTE_STORE_SYSTEM, "testTable"), SimpleRecord.SCHEMA$.toString());

    staticConfigs.put(configAvroRelSchemaProviderDomain + String.format(ConfigBasedAvroRelSchemaProviderFactory.CFG_SOURCE_SCHEMA,
            TEST_REMOTE_STORE_SYSTEM, "Profile"), Profile.SCHEMA$.toString());

    staticConfigs.putAll(props);

    return staticConfigs;
  }
}
