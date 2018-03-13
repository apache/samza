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

package org.apache.samza.sql.runner;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.impl.ConfigBasedUdfResolver;
import org.apache.samza.sql.interfaces.RelSchemaProvider;
import org.apache.samza.sql.interfaces.RelSchemaProviderFactory;
import org.apache.samza.sql.interfaces.SamzaRelConverter;
import org.apache.samza.sql.interfaces.SamzaRelConverterFactory;
import org.apache.samza.sql.interfaces.SourceResolver;
import org.apache.samza.sql.interfaces.SourceResolverFactory;
import org.apache.samza.sql.interfaces.SqlSystemSourceConfig;
import org.apache.samza.sql.interfaces.UdfMetadata;
import org.apache.samza.sql.interfaces.UdfResolver;
import org.apache.samza.sql.testutil.JsonUtil;
import org.apache.samza.sql.testutil.ReflectionUtils;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser;
import org.apache.samza.sql.testutil.SamzaSqlQueryParser.QueryInfo;
import org.apache.samza.sql.testutil.SqlFileParser;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class representing the Samza SQL application config
 */
public class SamzaSqlApplicationConfig {

  private static final Logger LOG = LoggerFactory.getLogger(SamzaSqlApplicationConfig.class);
  public static final String CFG_SQL_STMT = "samza.sql.stmt";
  public static final String CFG_SQL_STMTS_JSON = "samza.sql.stmts.json";
  public static final String CFG_SQL_FILE = "samza.sql.sqlFile";

  public static final String CFG_UDF_CONFIG_DOMAIN = "samza.sql.udf";

  public static final String CFG_FACTORY = "factory";

  public static final String CFG_FMT_REL_SCHEMA_PROVIDER_DOMAIN = "samza.sql.relSchemaProvider.%s.";
  public static final String CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN = "samza.sql.relConverter.%s.";

  public static final String CFG_SOURCE_RESOLVER = "samza.sql.sourceResolver";
  public static final String CFG_FMT_SOURCE_RESOLVER_DOMAIN = "samza.sql.sourceResolver.%s.";

  public static final String CFG_UDF_RESOLVER = "samza.sql.udfResolver";
  public static final String CFG_FMT_UDF_RESOLVER_DOMAIN = "samza.sql.udfResolver.%s.";
  private final Map<String, RelSchemaProvider> relSchemaProvidersBySource;
  private final Map<String, SamzaRelConverter> samzaRelConvertersBySource;

  private SourceResolver sourceResolver;
  private UdfResolver udfResolver;

  private final Collection<UdfMetadata> udfMetadata;

  private final Map<String, SqlSystemSourceConfig> inputSystemStreamConfigBySource;
  private final Map<String, SqlSystemSourceConfig> outputSystemStreamConfigsBySource;

  private final List<String> sql;

  private final List<QueryInfo> queryInfo;

  public SamzaSqlApplicationConfig(Config staticConfig) {

    sql = fetchSqlFromConfig(staticConfig);

    queryInfo = fetchQueryInfo(sql);

    sourceResolver = createSourceResolver(staticConfig);

    udfResolver = createUdfResolver(staticConfig);
    udfMetadata = udfResolver.getUdfs();

    inputSystemStreamConfigBySource = queryInfo.stream()
        .map(QueryInfo::getInputSources)
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(Function.identity(), sourceResolver::fetchSourceInfo));

    Set<SqlSystemSourceConfig> systemStreamConfigs = new HashSet<>(inputSystemStreamConfigBySource.values());

    outputSystemStreamConfigsBySource = queryInfo.stream()
        .map(QueryInfo::getOutputSource)
        .collect(Collectors.toMap(Function.identity(), x -> sourceResolver.fetchSourceInfo(x)));
    systemStreamConfigs.addAll(outputSystemStreamConfigsBySource.values());

    relSchemaProvidersBySource = systemStreamConfigs.stream()
        .collect(Collectors.toMap(SqlSystemSourceConfig::getSource,
            x -> initializePlugin("RelSchemaProvider", x.getRelSchemaProviderName(), staticConfig,
                CFG_FMT_REL_SCHEMA_PROVIDER_DOMAIN,
                (o, c) -> ((RelSchemaProviderFactory) o).create(x.getSystemStream(), c))));

    samzaRelConvertersBySource = systemStreamConfigs.stream()
        .collect(Collectors.toMap(SqlSystemSourceConfig::getSource,
            x -> initializePlugin("SamzaRelConverter", x.getSamzaRelConverterName(), staticConfig,
                CFG_FMT_SAMZA_REL_CONVERTER_DOMAIN, (o, c) -> ((SamzaRelConverterFactory) o).create(x.getSystemStream(),
                    relSchemaProvidersBySource.get(x.getSource()), c))));
  }

  private static <T> T initializePlugin(String pluginName, String plugin, Config staticConfig,
      String pluginDomainFormat, BiFunction<Object, Config, T> factoryInvoker) {
    String pluginDomain = String.format(pluginDomainFormat, plugin);
    Config pluginConfig = staticConfig.subset(pluginDomain);
    String factoryName = pluginConfig.getOrDefault(CFG_FACTORY, "");
    Validate.notEmpty(factoryName, String.format("Factory is not set for %s", plugin));
    Object factory = ReflectionUtils.createInstance(factoryName);
    Validate.notNull(factory, String.format("Factory creation failed for %s", plugin));
    LOG.info("Instantiating {} using factory {} with props {}", pluginName, factoryName, pluginConfig);
    return factoryInvoker.apply(factory, pluginConfig);
  }

  public static List<QueryInfo> fetchQueryInfo(List<String> sqlStmts) {
    return sqlStmts.stream().map(SamzaSqlQueryParser::parseQuery).collect(Collectors.toList());
  }

  public static List<String> fetchSqlFromConfig(Map<String, String> config) {
    List<String> sql;
    if (config.containsKey(CFG_SQL_STMT) && StringUtils.isNotBlank(config.get(CFG_SQL_STMT))) {
      String sqlValue = config.get(CFG_SQL_STMT);
      sql = Collections.singletonList(sqlValue);
    } else if (config.containsKey(CFG_SQL_STMTS_JSON) && StringUtils.isNotBlank(config.get(CFG_SQL_STMTS_JSON))) {
      sql = deserializeSqlStmts(config.get(CFG_SQL_STMTS_JSON));
    } else if (config.containsKey(CFG_SQL_FILE)) {
      String sqlFile = config.get(CFG_SQL_FILE);
      sql = SqlFileParser.parseSqlFile(sqlFile);
    } else {
      String msg = "Config doesn't contain the SQL that needs to be executed.";
      LOG.error(msg);
      throw new SamzaException(msg);
    }

    return sql;
  }

  private static List<String> deserializeSqlStmts(String value) {
    Validate.notEmpty(value, "json Value is not set or empty");
    return JsonUtil.fromJson(value, new TypeReference<List<String>>() {
    });
  }

  public static String serializeSqlStmts(List<String> sqlStmts) {
    Validate.notEmpty(sqlStmts, "json Value is not set or empty");
    return JsonUtil.toJson(sqlStmts);
  }

  public static SourceResolver createSourceResolver(Config config) {
    String sourceResolveValue = config.get(CFG_SOURCE_RESOLVER);
    Validate.notEmpty(sourceResolveValue, "sourceResolver config is not set or empty");
    return initializePlugin("SourceResolver", sourceResolveValue, config, CFG_FMT_SOURCE_RESOLVER_DOMAIN,
        (o, c) -> ((SourceResolverFactory) o).create(c));
  }

  private UdfResolver createUdfResolver(Map<String, String> config) {
    String udfResolveValue = config.get(CFG_UDF_RESOLVER);
    Validate.notEmpty(udfResolveValue, "udfResolver config is not set or empty");
    HashMap<String, String> domainConfig =
        getDomainProperties(config, String.format(CFG_FMT_UDF_RESOLVER_DOMAIN, udfResolveValue), false);
    Properties props = new Properties();
    props.putAll(domainConfig);
    HashMap<String, String> udfConfig = getDomainProperties(config, CFG_UDF_CONFIG_DOMAIN, false);
    return new ConfigBasedUdfResolver(props, new MapConfig(udfConfig));
  }

  private static HashMap<String, String> getDomainProperties(Map<String, String> props, String prefix,
      boolean preserveFullKey) {
    String fullPrefix;
    if (StringUtils.isBlank(prefix)) {
      fullPrefix = ""; // this will effectively retrieve all properties
    } else {
      fullPrefix = prefix.endsWith(".") ? prefix : prefix + ".";
    }
    HashMap<String, String> ret = new HashMap<>();
    props.keySet().forEach(keyStr -> {
      if (keyStr.startsWith(fullPrefix) && !keyStr.equals(fullPrefix)) {
        if (preserveFullKey) {
          ret.put(keyStr, props.get(keyStr));
        } else {
          ret.put(keyStr.substring(fullPrefix.length()), props.get(keyStr));
        }
      }
    });
    return ret;
  }

  public List<String> getSql() {
    return sql;
  }

  public List<QueryInfo> getQueryInfo() {
    return queryInfo;
  }

  public Collection<UdfMetadata> getUdfMetadata() {
    return udfMetadata;
  }

  public Map<String, SqlSystemSourceConfig> getInputSystemStreamConfigBySource() {
    return inputSystemStreamConfigBySource;
  }

  public Map<String, SqlSystemSourceConfig> getOutputSystemStreamConfigsBySource() {
    return outputSystemStreamConfigsBySource;
  }

  public Map<String, SamzaRelConverter> getSamzaRelConverters() {
    return samzaRelConvertersBySource;
  }

  public Map<String, RelSchemaProvider> getRelSchemaProviders() {
    return relSchemaProvidersBySource;
  }

  public SourceResolver getSourceResolver() {
    return sourceResolver;
  }
}
