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

package org.apache.samza.system.azureblob;

import org.apache.samza.system.azureblob.compression.CompressionType;
import java.time.Duration;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.config.MapConfig;

public class AzureBlobConfig extends MapConfig {
  private static final String SYSTEM_AZUREBLOB_PREFIX = "systems.%s.azureblob.";
  //Server Instance Level Property

  // The duration after which an Azure request will be logged as a warning.
  public static final String AZURE_BLOB_LOG_SLOW_REQUESTS_MS = "samza.azureblob.log.slowRequestMs";
  private static final long AZURE_BLOB_LOG_SLOW_REQUESTS_MS_DEFAULT = Duration.ofSeconds(30).toMillis();

  // Azure authentication - via a/c name+key or ClientSecretCredential
  // system Level Properties.
  // fully qualified class name of the AzureBlobWriter impl for the producer system
  public static final String SYSTEM_WRITER_FACTORY_CLASS_NAME = SYSTEM_AZUREBLOB_PREFIX + "writer.factory.class";
  public static final String SYSTEM_WRITER_FACTORY_CLASS_NAME_DEFAULT = "org.apache.samza.system.azureblob.avro.AzureBlobAvroWriterFactory";

  public static final String SYSTEM_USE_TOKEN_CREDENTIAL_AUTHENTICATION = Config.SENSITIVE_PREFIX + SYSTEM_AZUREBLOB_PREFIX + "useTokenCredentialAuthentication";
  private static final boolean SYSTEM_USE_TOKEN_CREDENTIAL_AUTHENTICATION_DEFAULT = false;

  // ClientSecretCredential needs client id, client secret, tenant id, vault name, service principal
  public static final String SYSTEM_AZURE_CLIENT_ID = Config.SENSITIVE_PREFIX + SYSTEM_AZUREBLOB_PREFIX + "client.id";
  public static final String SYSTEM_AZURE_CLIENT_SECRET = Config.SENSITIVE_PREFIX + SYSTEM_AZUREBLOB_PREFIX + "client.secret";
  public static final String SYSTEM_AZURE_TENANT_ID = Config.SENSITIVE_PREFIX + SYSTEM_AZUREBLOB_PREFIX + "tenant.id";
  // Whether to use proxy while authenticating with Azure
  public static final String SYSTEM_AZURE_USE_AUTH_PROXY  = SYSTEM_AZUREBLOB_PREFIX + "authProxy.use";
  public static final boolean SYSTEM_AZURE_USE_AUTH_PROXY_DEFAULT = false;

  // name of the host to be used as auth proxy
  public static final String SYSTEM_AZURE_AUTH_PROXY_HOSTNAME = SYSTEM_AZUREBLOB_PREFIX + "authProxy.hostname";
  // port in the auth proxy host to be used
  public static final String SYSTEM_AZURE_AUTH_PROXY_PORT = SYSTEM_AZUREBLOB_PREFIX + "authProxy.port";
  // Azure Storage Account name under which the Azure container representing this system is.
  // System name = Azure container name
  // (https://docs.microsoft.com/en-us/rest/api/storageservices/naming-and-referencing-containers--blobs--and-metadata#container-names)
  public static final String SYSTEM_AZURE_ACCOUNT_NAME = Config.SENSITIVE_PREFIX + SYSTEM_AZUREBLOB_PREFIX + "account.name";

  // Azure Storage Account key associated with the Azure Storage Account
  public static final String SYSTEM_AZURE_ACCOUNT_KEY  = Config.SENSITIVE_PREFIX + SYSTEM_AZUREBLOB_PREFIX + "account.key";

  // Whether to use proxy while connecting to Azure Storage
  public static final String SYSTEM_AZURE_USE_PROXY  = SYSTEM_AZUREBLOB_PREFIX + "proxy.use";
  public static final boolean SYSTEM_AZURE_USE_PROXY_DEFAULT = false;

  // name of the host to be used as proxy
  public static final String SYSTEM_AZURE_PROXY_HOSTNAME = SYSTEM_AZUREBLOB_PREFIX + "proxy.hostname";

  // port in the proxy host to be used
  public static final String SYSTEM_AZURE_PROXY_PORT = SYSTEM_AZUREBLOB_PREFIX + "proxy.port";

  // type of compression to be used before uploading blocks : “none” or “gzip”
  public static final String SYSTEM_COMPRESSION_TYPE = SYSTEM_AZUREBLOB_PREFIX + "compression.type";
  private static final CompressionType SYSTEM_COMPRESSION_TYPE_DEFAULT = CompressionType.NONE;

  // maximum size of uncompressed block in bytes
  public static final String SYSTEM_MAX_FLUSH_THRESHOLD_SIZE = SYSTEM_AZUREBLOB_PREFIX + "maxFlushThresholdSize";
  private static final int SYSTEM_MAX_FLUSH_THRESHOLD_SIZE_DEFAULT = 10485760;

  // maximum size of uncompressed blob in bytes
  public static final String SYSTEM_MAX_BLOB_SIZE = SYSTEM_AZUREBLOB_PREFIX + "maxBlobSize";
  private static final long SYSTEM_MAX_BLOB_SIZE_DEFAULT = Long.MAX_VALUE; // unlimited

  // maximum number of messages in a blob
  public static final String SYSTEM_MAX_MESSAGES_PER_BLOB = SYSTEM_AZUREBLOB_PREFIX + "maxMessagesPerBlob";
  private static final long SYSTEM_MAX_MESSAGES_PER_BLOB_DEFAULT = Long.MAX_VALUE; // unlimited

  // number of threads to asynchronously upload blocks
  public static final String SYSTEM_THREAD_POOL_COUNT = SYSTEM_AZUREBLOB_PREFIX + "threadPoolCount";
  private static final int SYSTEM_THREAD_POOL_COUNT_DEFAULT = 1;

  // size of the queue to hold blocks ready to be uploaded by asynchronous threads.
  // If all threads are busy uploading then blocks are queued and if queue is full then main thread will start uploading
  // which will block processing of incoming messages
  // Default - Thread Pool Count * 2
  public static final String SYSTEM_BLOCKING_QUEUE_SIZE = SYSTEM_AZUREBLOB_PREFIX + "blockingQueueSize";

  // timeout to finish uploading all blocks before committing a blob
  public static final String SYSTEM_FLUSH_TIMEOUT_MS = SYSTEM_AZUREBLOB_PREFIX + "flushTimeoutMs";
  private static final long SYSTEM_FLUSH_TIMEOUT_MS_DEFAULT = Duration.ofMinutes(3).toMillis();

  // timeout to finish committing all the blobs currently being written to. This does not include the flush timeout per blob
  public static final String SYSTEM_CLOSE_TIMEOUT_MS = SYSTEM_AZUREBLOB_PREFIX + "closeTimeoutMs";
  private static final long SYSTEM_CLOSE_TIMEOUT_MS_DEFAULT = Duration.ofMinutes(5).toMillis();

  // if true, a random string of 8 chars is suffixed to the blob name to prevent name collision
  // when more than one Samza tasks are writing to the same SSP.
  public static final String SYSTEM_SUFFIX_RANDOM_STRING_TO_BLOB_NAME = SYSTEM_AZUREBLOB_PREFIX + "suffixRandomStringToBlobName";
  private static final boolean SYSTEM_SUFFIX_RANDOM_STRING_TO_BLOB_NAME_DEFAULT = true;

  // full class name of an implementation of org.apache.samza.system.azureblob.utils.BlobMetadataGeneratorFactory
  // this factory should return an implementation of org.apache.samza.system.azureblob.utils.BlobMetadataGenerator
  // this generator will be invoked when a blob is committed to add metadata properties to it
  public static final String SYSTEM_BLOB_METADATA_PROPERTIES_GENERATOR_FACTORY = SYSTEM_AZUREBLOB_PREFIX + "metadataPropertiesGeneratorFactory";
  private static final String SYSTEM_BLOB_METADATA_PROPERTIES_GENERATOR_FACTORY_DEFAULT =
      "org.apache.samza.system.azureblob.utils.NullBlobMetadataGeneratorFactory";
  // Additional configs for the metadata generator should be prefixed with this string which is passed to the generator.
  // for example, to pass a "key":"value" pair to the metadata generator, add config like
  // systems.<system-name>.azureblob.metadataGeneratorConfig.<key> with value <value>
  public static final String SYSTEM_BLOB_METADATA_GENERATOR_CONFIG_PREFIX = SYSTEM_AZUREBLOB_PREFIX + "metadataGeneratorConfig.";

  public AzureBlobConfig(Config config) {
    super(config);
  }

  public String getAzureAccountKey(String systemName) {
    String accountKey = get(String.format(SYSTEM_AZURE_ACCOUNT_KEY, systemName));
    if (accountKey == null) {
      throw new ConfigException("Azure account key is required.");
    }
    return accountKey;
  }

  public String getAzureAccountName(String systemName) {
    String accountName = get(String.format(SYSTEM_AZURE_ACCOUNT_NAME, systemName));
    if (accountName == null) {
      throw new ConfigException("Azure account name is required.");
    }
    return accountName;
  }

  public boolean getUseBlobProxy(String systemName) {
    return getBoolean(String.format(SYSTEM_AZURE_USE_PROXY, systemName), SYSTEM_AZURE_USE_PROXY_DEFAULT);
  }

  public String getAzureBlobProxyHostname(String systemName) {
    String hostname = get(String.format(SYSTEM_AZURE_PROXY_HOSTNAME, systemName));
    if (hostname == null) {
      throw new ConfigException("Azure proxy host name is required.");
    }
    return hostname;
  }

  public int getAzureBlobProxyPort(String systemName) {
    return getInt(String.format(SYSTEM_AZURE_PROXY_PORT, systemName));
  }

  public CompressionType getCompressionType(String systemName) {
    return CompressionType.valueOf(get(String.format(SYSTEM_COMPRESSION_TYPE, systemName), SYSTEM_COMPRESSION_TYPE_DEFAULT.name()).toUpperCase());
  }

  public String getAzureBlobWriterFactoryClassName(String systemName) {
    return get(String.format(SYSTEM_WRITER_FACTORY_CLASS_NAME, systemName), SYSTEM_WRITER_FACTORY_CLASS_NAME_DEFAULT);
  }

  public int getMaxFlushThresholdSize(String systemName) {
    return getInt(String.format(SYSTEM_MAX_FLUSH_THRESHOLD_SIZE, systemName), SYSTEM_MAX_FLUSH_THRESHOLD_SIZE_DEFAULT);
  }

  public int getAzureBlobThreadPoolCount(String systemName) {
    return getInt(String.format(SYSTEM_THREAD_POOL_COUNT, systemName), SYSTEM_THREAD_POOL_COUNT_DEFAULT);
  }

  public int getBlockingQueueSize(String systemName) {
    return getInt(String.format(SYSTEM_BLOCKING_QUEUE_SIZE, systemName), 2 * getAzureBlobThreadPoolCount(systemName));
  }

  public long getFlushTimeoutMs(String systemName) {
    long timeout = getLong(String.format(SYSTEM_FLUSH_TIMEOUT_MS, systemName), SYSTEM_FLUSH_TIMEOUT_MS_DEFAULT);
    if (timeout <= 0) {
      throw new ConfigException("Azure Blob flush timeout can not be <= 0");
    }
    return timeout;
  }

  public long getCloseTimeoutMs(String systemName) {
    long timeout = getLong(String.format(SYSTEM_CLOSE_TIMEOUT_MS, systemName), SYSTEM_CLOSE_TIMEOUT_MS_DEFAULT);
    if (timeout <= 0) {
      throw new ConfigException("Azure Blob close timeout can not be <= 0");
    }
    return timeout;
  }

  public long getLogSlowRequestsMs() {
    long duration = getLong(AZURE_BLOB_LOG_SLOW_REQUESTS_MS, AZURE_BLOB_LOG_SLOW_REQUESTS_MS_DEFAULT);
    if (duration <= 0) {
      throw new ConfigException("Azure blob duration to log slow requests can not be <=0.");
    }
    return duration;
  }

  public boolean getSuffixRandomStringToBlobName(String systemName) {
    return getBoolean(String.format(SYSTEM_SUFFIX_RANDOM_STRING_TO_BLOB_NAME, systemName), SYSTEM_SUFFIX_RANDOM_STRING_TO_BLOB_NAME_DEFAULT);
  }

  public long getMaxBlobSize(String systemName) {
    return getLong(String.format(SYSTEM_MAX_BLOB_SIZE, systemName), SYSTEM_MAX_BLOB_SIZE_DEFAULT);
  }

  public long getMaxMessagesPerBlob(String systemName) {
    return getLong(String.format(SYSTEM_MAX_MESSAGES_PER_BLOB, systemName), SYSTEM_MAX_MESSAGES_PER_BLOB_DEFAULT);
  }

  public String getSystemBlobMetadataPropertiesGeneratorFactory(String systemName) {
    return get(String.format(SYSTEM_BLOB_METADATA_PROPERTIES_GENERATOR_FACTORY, systemName),
        SYSTEM_BLOB_METADATA_PROPERTIES_GENERATOR_FACTORY_DEFAULT);
  }

  public Config getSystemBlobMetadataGeneratorConfigs(String systemName) {
    return subset(String.format(SYSTEM_BLOB_METADATA_GENERATOR_CONFIG_PREFIX, systemName));
  }

  public boolean getUseTokenCredentialAuthentication(String systemName) {
    return getBoolean(String.format(SYSTEM_USE_TOKEN_CREDENTIAL_AUTHENTICATION, systemName),
        SYSTEM_USE_TOKEN_CREDENTIAL_AUTHENTICATION_DEFAULT);
  }

  public boolean getUseAuthProxy(String systemName) {
    return getBoolean(String.format(SYSTEM_AZURE_USE_AUTH_PROXY, systemName), SYSTEM_AZURE_USE_AUTH_PROXY_DEFAULT);
  }

  public String getAuthProxyHostName(String systemName) {
    String hostname = get(String.format(SYSTEM_AZURE_AUTH_PROXY_HOSTNAME, systemName));
    if (hostname == null) {
      throw new ConfigException("Azure proxy host name is required.");
    }
    return hostname;
  }

  public int getAuthProxyPort(String systemName) {
    return getInt(String.format(SYSTEM_AZURE_AUTH_PROXY_PORT, systemName));
  }

  public String getAzureClientId(String systemName) {
    String clientId = get(String.format(SYSTEM_AZURE_CLIENT_ID, systemName));
    if (clientId == null) {
      throw new ConfigException("Azure Client id is required.");
    }
    return clientId;
  }

  public String getAzureClientSecret(String systemName) {
    String clientSecret = get(String.format(SYSTEM_AZURE_CLIENT_SECRET, systemName));
    if (clientSecret == null) {
      throw new ConfigException("Azure Client secret is required.");
    }
    return clientSecret;
  }


  public String getAzureTenantId(String systemName) {
    String tenantId = get(String.format(SYSTEM_AZURE_TENANT_ID, systemName));
    if (tenantId == null) {
      throw new ConfigException("Azure tenant id is required.");
    }
    return tenantId;
  }
}
