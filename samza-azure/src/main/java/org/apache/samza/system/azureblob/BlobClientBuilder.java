package org.apache.samza.system.azureblob;

import com.azure.storage.blob.BlobServiceAsyncClient;


/**
 * Create a BlobServiceAsyncClient. Implementation controls construction of
 * underlying client.
 */
public interface BlobClientBuilder {
  /**
   * create a client for ABS (uploads)
   */
  BlobServiceAsyncClient getBlobServiceAsyncClient();
}
