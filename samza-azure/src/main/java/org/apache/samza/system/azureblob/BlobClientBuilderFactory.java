package org.apache.samza.system.azureblob;

/**
 * Constructs a new instance of {@link AzureBlobClientBuilder}
 * from factory provided by configuration.
 */
public interface BlobClientBuilderFactory {
  /**
   * Create a new instance of {@link AzureBlobClientBuilder}
   * @param systemName Name of the system
   * @param azureUrlFormat Azure URL format
   * @param azureBlobConfig Azure Blob configuration
   * @return New instance of {@link AzureBlobClientBuilder}
   */
  BlobClientBuilder getBlobClientBuilder(String systemName, String azureUrlFormat, AzureBlobConfig azureBlobConfig);
}
