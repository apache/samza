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

package org.apache.samza.system.kinesis;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.ClientConfiguration;


/**
 * Configs for Kinesis system. It contains three sets of configs:
 * <ol>
 *   <li> Configs required by Samza Kinesis Consumer.
 *   <li> Configs that are AWS client specific provided at system scope {@link ClientConfiguration}
 *   <li> Configs that are KCL specific (could be provided either at system scope or stream scope)
 *        {@link KinesisClientLibConfiguration}
 * </ol>
 */
public class KinesisConfig extends MapConfig {
  private static final Logger LOG = LoggerFactory.getLogger(KinesisConfig.class.getName());

  private static final String CONFIG_SYSTEM_REGION = "systems.%s.aws.region";
  private static final String CONFIG_STREAM_REGION = "systems.%s.streams.%s.aws.region";

  private static final String CONFIG_STREAM_ACCESS_KEY = "systems.%s.streams.%s.aws.accessKey";
  private static final String CONFIG_STREAM_SECRET_KEY = "sensitive.systems.%s.streams.%s.aws.secretKey";

  private static final String CONFIG_AWS_CLIENT_CONFIG = "systems.%s.aws.clientConfig.";
  private static final String CONFIG_PROXY_HOST = CONFIG_AWS_CLIENT_CONFIG + "ProxyHost";
  private static final String DEFAULT_CONFIG_PROXY_HOST = "";
  private static final String CONFIG_PROXY_PORT = CONFIG_AWS_CLIENT_CONFIG + "ProxyPort";
  private static final int DEFAULT_CONFIG_PROXY_PORT = 0;

  private static final String CONFIG_SYSTEM_KINESIS_CLIENT_LIB_CONFIG = "systems.%s.aws.kcl.";
  private static final String CONFIG_STREAM_KINESIS_CLIENT_LIB_CONFIG = "systems.%s.streams.%s.aws.kcl.";

  public KinesisConfig(Config config) {
    super(config);
  }

  /**
   * Return a set of streams from the config for a given system.
   * @param system name of the system
   * @return a set of streams
   */
  public Set<String> getKinesisStreams(String system) {
    // getOperatorSpecGraph stream-level configs
    Config streamsConfig = subset(String.format("systems.%s.streams.", system), true);
    // all properties should now start with stream name
    Set<String> streams = new HashSet<>();
    streamsConfig.keySet().forEach(key -> {
        String[] parts = key.split("\\.", 2);
        if (parts.length != 2) {
          throw new IllegalArgumentException("Ill-formatted stream config: " + key);
        }
        streams.add(parts[0]);
      });
    return streams;
  }

  /**
   * Get KCL config for a given system stream.
   * @param system name of the system
   * @param stream name of the stream
   * @param appName name of the application
   * @return Stream scoped KCL configs required to getOperatorSpecGraph
   *         {@link KinesisClientLibConfiguration}
   */
  public KinesisClientLibConfiguration getKinesisClientLibConfig(String system, String stream, String appName) {
    ClientConfiguration clientConfig = getAWSClientConfig(system);
    String workerId = appName + "-" + UUID.randomUUID();
    InitialPositionInStream startPos = InitialPositionInStream.LATEST;
    AWSCredentialsProvider provider = credentialsProviderForStream(system, stream);
    KinesisClientLibConfiguration kinesisClientLibConfiguration =
        new KinesisClientLibConfiguration(appName, stream, provider, workerId)
            .withRegionName(getRegion(system, stream).getName())
            .withKinesisClientConfig(clientConfig)
            .withCloudWatchClientConfig(clientConfig)
            .withDynamoDBClientConfig(clientConfig)
            .withInitialPositionInStream(startPos)
            .withCallProcessRecordsEvenForEmptyRecordList(true); // For health monitoring metrics.
    // First, get system scoped configs for KCL and override with configs set at stream scope.
    setKinesisClientLibConfigs(
        subset(String.format(CONFIG_SYSTEM_KINESIS_CLIENT_LIB_CONFIG, system)), kinesisClientLibConfiguration);
    setKinesisClientLibConfigs(subset(String.format(CONFIG_STREAM_KINESIS_CLIENT_LIB_CONFIG, system, stream)),
        kinesisClientLibConfiguration);
    return kinesisClientLibConfiguration;
  }

  /**
   * Get the Kinesis secret key for the system stream
   * @param system name of the system
   * @param stream name of the stream
   * @return Kinesis secret key
   */
  protected String getStreamSecretKey(String system, String stream) {
    return get(String.format(CONFIG_STREAM_SECRET_KEY, system, stream));
  }

  /**
   * Get SSL socket factory for the proxy for a given system
   * @param system name of the system
   * @return ConnectionSocketFactory
   */
  protected ConnectionSocketFactory getSSLSocketFactory(String system) {
    return null;
  }

  /**
   * Get the proxy host as a system level config. This is needed when
   * users need to go through a proxy for the Kinesis connections.
   * @param system name of the system
   * @return proxy host name or empty string if not defined
   */
  protected String getProxyHost(String system) {
    return get(String.format(CONFIG_PROXY_HOST, system), DEFAULT_CONFIG_PROXY_HOST);
  }

  /**
   * Get the proxy port number as a system level config. This is needed when
   * users need to go through a proxy for the Kinesis connections.
   * @param system name of the system
   * @return proxy port number or 0 if not defined
   */
  protected int getProxyPort(String system) {
    return getInt(String.format(CONFIG_PROXY_PORT, system), DEFAULT_CONFIG_PROXY_PORT);
  }

  /**
   * @param system name of the system
   * @return {@link ClientConfiguration} which has options controlling how the client connects to kinesis
   *         (eg: proxy settings, retry counts, etc)
   */
  ClientConfiguration getAWSClientConfig(String system) {
    ClientConfiguration awsClientConfig = new ClientConfiguration();
    setAwsClientConfigs(subset(String.format(CONFIG_AWS_CLIENT_CONFIG, system)), awsClientConfig);
    awsClientConfig.getApacheHttpClientConfig().setSslSocketFactory(getSSLSocketFactory(system));
    return awsClientConfig;
  }

  /**
   * Get the Kinesis region for the system stream
   * @param system name of the system
   * @param stream name of the stream
   * @return Kinesis region
   */
  Region getRegion(String system, String stream) {
    String name = get(String.format(CONFIG_STREAM_REGION, system, stream),
        get(String.format(CONFIG_SYSTEM_REGION, system)));
    return Region.getRegion(Regions.fromName(name));
  }

  /**
   * Get the Kinesis access key name for the system stream
   * @param system name of the system
   * @param stream name of the stream
   * @return Kinesis access key
   */
  String getStreamAccessKey(String system, String stream) {
    return get(String.format(CONFIG_STREAM_ACCESS_KEY, system, stream));
  }

  /**
   * Get the appropriate CredentialProvider for a given system stream.
   * @param system name of the system
   * @param stream name of the stream
   * @return AWSCredentialsProvider
   */
  AWSCredentialsProvider credentialsProviderForStream(String system, String stream) {
    // Try to load credentials in the following order:
    // 1. Access key from the config and passed in secretKey
    // 2. From the default credential provider chain (environment variables, system properties, AWS profile file, etc)
    return new AWSCredentialsProviderChain(
        new KinesisAWSCredentialsProvider(getStreamAccessKey(system, stream), getStreamSecretKey(system, stream)),
        new DefaultAWSCredentialsProviderChain());
  }

  private void setAwsClientConfigs(Config config, ClientConfiguration clientConfig) {
    for (Entry<String, String> entry : config.entrySet()) {
      boolean found = false;
      String key = entry.getKey();
      String value = entry.getValue();
      if (StringUtils.isEmpty(value)) {
        continue;
      }
      for (Method method : ClientConfiguration.class.getMethods()) {
        // For each property invoke the corresponding setter, if it exists
        if (method.getName().equals("set" + key)) {
          found = true;
          Class<?> type = method.getParameterTypes()[0];
          try {
            if (type == long.class) {
              method.invoke(clientConfig, Long.valueOf(value));
            } else if (type == int.class) {
              method.invoke(clientConfig, Integer.valueOf(value));
            } else if (type == boolean.class) {
              method.invoke(clientConfig, Boolean.valueOf(value));
            } else if (type == String.class) {
              method.invoke(clientConfig, value);
            }
            LOG.info("Loaded property " + key + " = " + value);
            break;
          } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Error trying to set field %s with the value '%s'", key, value), e);
          }
        }
      }
      if (!found) {
        LOG.warn("Property " + key + " ignored as there is no corresponding set method");
      }
    }
  }

  private void setKinesisClientLibConfigs(Map<String, String> config, KinesisClientLibConfiguration kinesisLibConfig) {
    for (Entry<String, String> entry : config.entrySet()) {
      boolean found = false;
      String key = entry.getKey();
      String value = entry.getValue();
      if (StringUtils.isEmpty(value)) {
        continue;
      }
      for (Method method : KinesisClientLibConfiguration.class.getMethods()) {
        if (method.getName().equals("with" + key)) {
          found = true;
          Class<?> type = method.getParameterTypes()[0];
          try {
            if (type == long.class) {
              method.invoke(kinesisLibConfig, Long.valueOf(value));
            } else if (type == int.class) {
              method.invoke(kinesisLibConfig, Integer.valueOf(value));
            } else if (type == boolean.class) {
              method.invoke(kinesisLibConfig, Boolean.valueOf(value));
            } else if (type == String.class) {
              method.invoke(kinesisLibConfig, value);
            } else if (type == InitialPositionInStream.class) {
              method.invoke(kinesisLibConfig, InitialPositionInStream.valueOf(value.toUpperCase()));
            }
            LOG.info("Loaded property " + key + " = " + value);
            break;
          } catch (Exception e) {
            throw new IllegalArgumentException(
                String.format("Error trying to set field %s with the value '%s'", key, value), e);
          }
        }
      }
      if (!found) {
        LOG.warn("Property " + key + " ignored as there is no corresponding set method");
      }
    }
  }
}
