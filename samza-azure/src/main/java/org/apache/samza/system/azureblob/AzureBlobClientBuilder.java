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

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.policy.HttpLogDetailLevel;
import com.azure.core.http.policy.HttpLogOptions;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.net.InetSocketAddress;
import com.azure.core.http.HttpClient;
import java.util.Locale;


/**
 * This class provides a method to create {@link BlobServiceAsyncClient} to be used by the
 * {@link org.apache.samza.system.azureblob.producer.AzureBlobSystemProducer}. It created the client based on the
 * configs given to the SystemProducer - such as which authentication method to use, whether to use proxy to authenticate,
 * and so on.
 */
public final class AzureBlobClientBuilder {
  private final String systemName;
  private final String azureUrlFormat;
  private final AzureBlobConfig azureBlobConfig;
  public AzureBlobClientBuilder(String systemName, String azureUrlFormat, AzureBlobConfig azureBlobConfig) {
    this.systemName = systemName;
    this.azureUrlFormat = azureUrlFormat;
    this.azureBlobConfig = azureBlobConfig;
  }

  /**
   * method creates BlobServiceAsyncClient using the configs provided earlier.
   * if the authentication method is set to {@link TokenCredential} then a {@link com.azure.identity.ClientSecretCredential}
   * is created and used for the Blob client. Else authentication is done via account name and key using the
   * {@link StorageSharedKeyCredential}.
   * The config used to determine which authentication is systems.%s.azureblob.useTokenCredentialAuthentication = true
   * for using TokenCredential.
   * @return BlobServiceAsyncClient
   */
  public BlobServiceAsyncClient getBlobServiceAsyncClient() {
    BlobServiceClientBuilder blobServiceClientBuilder = getBlobServiceClientBuilder();

    if (azureBlobConfig.getUseTokenCredentialAuthentication(systemName)) {
      // Use your Azure Blob Storage account's name and client details to create a token credential object to access your account.
      TokenCredential tokenCredential = getTokenCredential();
      return blobServiceClientBuilder.credential(tokenCredential).buildAsyncClient();
    }

    // Use your Azure Blob Storage account's name and key to create a credential object to access your account.
    StorageSharedKeyCredential storageSharedKeyCredential = getStorageSharedKeyCredential();
    return blobServiceClientBuilder.credential(storageSharedKeyCredential).buildAsyncClient();
  }

  /**
   * Method to get the builder {@link BlobServiceClientBuilder} for creating BlobServiceAsyncClient.
   * This builder is not provided the credential for authentication here.
   * this builder is given an endpoint for the Azure Storage account and a http client to be passed on to the
   * BlobServiceAsyncClient for blob creation.
   * @return BlobServiceClientBuilder
   */
  private BlobServiceClientBuilder getBlobServiceClientBuilder() {
    // From the Azure portal, get your Storage account blob service AsyncClient endpoint.
    String endpoint = String.format(Locale.ROOT, azureUrlFormat, azureBlobConfig.getAzureAccountName(systemName));

    HttpLogOptions httpLogOptions = new HttpLogOptions();
    httpLogOptions.setLogLevel(HttpLogDetailLevel.BASIC);

    BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder()
        .httpLogOptions(httpLogOptions)
        .endpoint(endpoint)
        .httpClient(getHttpClient());

    return blobServiceClientBuilder;
  }

  /**
   * Method to create the {@link com.azure.identity.ClientSecretCredential} to be used for authenticating with
   * Azure Storage. If the config systems.%s.azureblob.authProxy.use is set to true, then authentication happens
   * via the proxy specified by systems.%s.azureblob.authProxy.hostname and systems.%s.azureblob.authProxy.port configs.
   * @return ClientSecretCredential which extends from {@link TokenCredential}
   */
  private TokenCredential getTokenCredential() {
    ClientSecretCredentialBuilder clientSecretCredentialBuilder = new ClientSecretCredentialBuilder()
        .clientId(azureBlobConfig.getAzureClientId(systemName))
        .clientSecret(azureBlobConfig.getAzureClientSecret(systemName))
        .tenantId(azureBlobConfig.getAzureTenantId(systemName));

    if (azureBlobConfig.getUseAuthProxy(systemName)) {
      return clientSecretCredentialBuilder
          .proxyOptions(new ProxyOptions(ProxyOptions.Type.HTTP,
              new InetSocketAddress(azureBlobConfig.getAuthProxyHostName(systemName),
                  azureBlobConfig.getAuthProxyPort(systemName))))
          .build();
    }
    return clientSecretCredentialBuilder
        .build();
  }

  /**
   * Method to create {@link StorageSharedKeyCredential} to used for authenticating with Azure Storage.
   * @return StorageSharedKeyCredential
   */
  private StorageSharedKeyCredential getStorageSharedKeyCredential() {
    return new StorageSharedKeyCredential(azureBlobConfig.getAzureAccountName(systemName),
        azureBlobConfig.getAzureAccountKey(systemName));
  }

  /**
   * Method to create {@link HttpClient} to be used by the {@link BlobServiceAsyncClient} while creating blobs
   * in the Azure Storage. If the config systems.%s.azureblob.proxy.use is set to true then the http client
   * uses the proxy provided by systems.%s.azureblob.proxy.hostname and systems.%s.azureblob.proxy.port configs.
   * @return HttpClient
   */
  private HttpClient getHttpClient() {
    HttpClient httpClient;
    if (azureBlobConfig.getUseBlobProxy(systemName)) {
      httpClient = new NettyAsyncHttpClientBuilder()
          .proxy(new ProxyOptions(ProxyOptions.Type.HTTP,
              new InetSocketAddress(azureBlobConfig.getAzureBlobProxyHostname(systemName),
                  azureBlobConfig.getAzureBlobProxyPort(systemName))))
          .build();
    } else {
      httpClient = HttpClient.createDefault();
    }
    return httpClient;
  }
}
