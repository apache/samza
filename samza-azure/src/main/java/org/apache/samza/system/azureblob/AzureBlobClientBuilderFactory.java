package org.apache.samza.system.azureblob;

/**
 * Default implementation of {@link BlobClientBuilderFactory} that constructs a
 * new instance of {@link AzureBlobClientBuilder}.
 */
public class AzureBlobClientBuilderFactory implements BlobClientBuilderFactory {
  @Override
  public BlobClientBuilder getBlobClientBuilder(String systemName, String azureUrlFormat,
      AzureBlobConfig azureBlobConfig) {
    return new AzureBlobClientBuilder(systemName, azureUrlFormat, azureBlobConfig);
  }
}
