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

package org.apache.samza;

import com.microsoft.azure.storage.AccessCondition;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudPageBlob;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.serializers.model.SamzaObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Client side class that has reference to Azure blob storage.
 * Used for writing and reading from the blob.
 * Every write requires a valid lease ID.
 */
public class BlobUtils {

  private static final Logger LOG = LoggerFactory.getLogger(BlobUtils.class);
  private static final long JOB_MODEL_BLOCK_SIZE = 1024000;
  private static final long BARRIER_STATE_BLOCK_SIZE = 1024;
  private static final long PROCESSOR_LIST_BLOCK_SIZE = 1024;
  private CloudBlobClient blobClient;
  private CloudBlobContainer container;
  private CloudPageBlob blob;

  /**
   * Creates an object of BlobUtils. It creates the container and page blob if they don't exist already.
   * @param client Client handle for access to Azure Storage account.
   * @param containerName Name of container inside which we want the blob to reside.
   * @param blobName Name of the blob to be managed.
   * @param length Length of the page blob.
   * @throws AzureException If an Azure storage service error occurred, or when the container name or blob name is invalid.
   */
  public BlobUtils(AzureClient client, String containerName, String blobName, long length) {
    this.blobClient = client.getBlobClient();
    try {
      this.container = blobClient.getContainerReference(containerName);
      container.createIfNotExists();
      this.blob = container.getPageBlobReference(blobName);
      if (!blob.exists()) {
        blob.create(length);
      }
    } catch (URISyntaxException | StorageException e) {
      LOG.error("Azure Exception!", e);
      throw new AzureException(e);
    }
  }

  /**
   * Writes the job model to the blob.
   * Write is successful only if the lease ID passed is valid and the processor holds the lease.
   * Called by the leader.
   * @param prevJM Previous job model version that the processor was operating on.
   * @param currJM Current job model version that the processor is operating on.
   * @param prevJMV Previous job model version that the processor was operating on.
   * @param currJMV Current job model version that the processor is operating on.
   * @param leaseId LeaseID of the lease that the processor holds on the blob. Null if there is no lease.
   * @return true if write to the blob is successful, false if leaseID is null or an Azure storage service error or IO exception occurred.
   */
  public boolean publishJobModel(JobModel prevJM, JobModel currJM, String prevJMV, String currJMV, String leaseId) {
    try {
      if (leaseId == null) {
        return false;
      }
      JobModelBundle bundle = new JobModelBundle(prevJM, currJM, prevJMV, currJMV);
      byte[] data = SamzaObjectMapper.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(bundle);
      byte[] pageData = Arrays.copyOf(data, (int) JOB_MODEL_BLOCK_SIZE);
      InputStream is = new ByteArrayInputStream(pageData);
      blob.uploadPages(is, 0, JOB_MODEL_BLOCK_SIZE, AccessCondition.generateLeaseCondition(leaseId), null, null);
      LOG.info("Uploaded {} jobModel to blob", bundle.getCurrJobModel());
      return true;
    } catch (StorageException | IOException e) {
      LOG.error("JobModel publish failed for version = " + currJMV, e);
      return false;
    }
  }

  /**
   * Reads the current job model from the blob.
   * @return The current job model published on the blob. Returns null when job model details not found on the blob.
   * @throws AzureException in getJobModelBundle() if an Azure storage service error occurred.
   * @throws SamzaException in getJobModelBundle() if data retrieved from blob could not be parsed by SamzaObjectMapper.
   */
  public JobModel getJobModel() {
    LOG.info("Reading the job model from blob.");
    JobModelBundle jmBundle = getJobModelBundle();
    if (jmBundle == null) {
      LOG.error("Job Model details don't exist on the blob.");
      return null;
    }
    JobModel jm = jmBundle.getCurrJobModel();
    return jm;
  }

  /**
   * Reads the current job model version from the blob .
   * @return Current job model version published on the blob. Returns null when job model details not found on the blob.
   * @throws AzureException in getJobModelBundle() if an Azure storage service error occurred.
   * @throws SamzaException in getJobModelBundle() if data retrieved from blob could not be parsed by SamzaObjectMapper.
   */
  public String getJobModelVersion() {
    LOG.info("Reading the job model version from blob.");
    JobModelBundle jmBundle = getJobModelBundle();
    if (jmBundle == null) {
      LOG.error("Job Model details don't exist on the blob.");
      return null;
    }
    String jmVersion = jmBundle.getCurrJobModelVersion();
    return jmVersion;
  }

  /**
   * Writes the barrier state to the blob.
   * Write is successful only if the lease ID passed is valid and the processor holds the lease.
   * Called only by the leader.
   * @param state Barrier state to be published to the blob.
   * @param leaseId LeaseID of the valid lease that the processor holds on the blob. Null if there is no lease.
   * @return true if write to the blob is successful, false if leaseID is null or an Azure storage service error or IO exception occurred.
   */
  public boolean publishBarrierState(String state, String leaseId) {
    try {
      if (leaseId == null) {
        return false;
      }
      byte[] data = SamzaObjectMapper.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
      byte[] pageData = Arrays.copyOf(data, (int) BARRIER_STATE_BLOCK_SIZE);
      InputStream is = new ByteArrayInputStream(pageData);

      //uploadPages is only successful when the AccessCondition provided has an active and valid lease ID. It fails otherwise.
      blob.uploadPages(is, JOB_MODEL_BLOCK_SIZE, BARRIER_STATE_BLOCK_SIZE, AccessCondition.generateLeaseCondition(leaseId), null, null);
      LOG.info("Uploaded barrier state {} to blob", state);
      return true;
    } catch (StorageException | IOException e) {
      LOG.error("Barrier state " + state + " publish failed", e);
      return false;
    }
  }

  /**
   * Reads the current barrier state from the blob.
   * @return Barrier state published on the blob.
   * @throws AzureException If an Azure storage service error occurred.
   * @throws SamzaException If data retrieved from blob could not be parsed by SamzaObjectMapper.
   */
  public String getBarrierState() {
    LOG.info("Reading the barrier state from blob.");
    byte[] data = new byte[(int) BARRIER_STATE_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(JOB_MODEL_BLOCK_SIZE, BARRIER_STATE_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      LOG.error("Failed to read barrier state from blob.", e);
      throw new AzureException(e);
    }
    String state;
    try {
      state = SamzaObjectMapper.getObjectMapper().readValue(data, String.class);
    } catch (IOException e) {
      LOG.error("Failed to parse byte data: " + data + " for barrier state retrieved from the blob.", e);
      throw new SamzaException(e);
    }
    return state;
  }

  /**
   * Writes the list of live processors in the system to the blob.
   * Write is successful only if the lease ID passed is valid and the processor holds the lease.
   * Called only by the leader.
   * @param processors List of live processors to be published on the blob.
   * @param leaseId LeaseID of the valid lease that the processor holds on the blob. Null if there is no lease.
   * @return true if write to the blob is successful, false if leaseID is null or an Azure storage service error or IO exception occurred.
   */
  public boolean publishLiveProcessorList(List<String> processors, String leaseId) {
    try {
      if (leaseId == null) {
        return false;
      }
      byte[] data = SamzaObjectMapper.getObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsBytes(processors);
      byte[] pageData = Arrays.copyOf(data, (int) BARRIER_STATE_BLOCK_SIZE);
      InputStream is = new ByteArrayInputStream(pageData);
      blob.uploadPages(is, JOB_MODEL_BLOCK_SIZE + BARRIER_STATE_BLOCK_SIZE, PROCESSOR_LIST_BLOCK_SIZE, AccessCondition.generateLeaseCondition(leaseId), null, null);
      LOG.info("Uploaded list of live processors to blob.");
      return true;
    } catch (StorageException | IOException e) {
      LOG.error("Processor list: " + processors + "publish failed", e);
      return false;
    }
  }

  /**
   * Reads the list of live processors published on the blob.
   * @return String list of live processors.
   * @throws AzureException If an Azure storage service error occurred.
   * @throws SamzaException If data retrieved from blob could not be parsed by SamzaObjectMapper.
   */
  public List<String> getLiveProcessorList() {
    LOG.info("Read the the list of live processors from blob.");
    byte[] data = new byte[(int) PROCESSOR_LIST_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(JOB_MODEL_BLOCK_SIZE + BARRIER_STATE_BLOCK_SIZE, PROCESSOR_LIST_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      LOG.error("Failed to read the list of live processors from the blob.", new AzureException(e));
      throw new AzureException(e);
    }
    List<String> list;
    try {
      list = SamzaObjectMapper.getObjectMapper().readValue(data, List.class);
    } catch (IOException e) {
      LOG.error("Failed to parse byte data: " + data + " for live processor list retrieved from the blob", new SamzaException(e));
      throw new SamzaException(e);
    }
    return list;
  }

  public CloudBlobClient getBlobClient() {
    return this.blobClient;
  }

  public CloudBlobContainer getBlobContainer() {
    return this.container;
  }

  public CloudPageBlob getBlob() {
    return this.blob;
  }

  private JobModelBundle getJobModelBundle() {
    byte[] data = new byte[(int) JOB_MODEL_BLOCK_SIZE];
    try {
      blob.downloadRangeToByteArray(0, JOB_MODEL_BLOCK_SIZE, data, 0);
    } catch (StorageException e) {
      LOG.error("Failed to read JobModel details from the blob.", e);
      throw new AzureException(e);
    }
    try {
      JobModelBundle jmBundle = SamzaObjectMapper.getObjectMapper().readValue(data, JobModelBundle.class);
      return jmBundle;
    } catch (IOException e) {
      LOG.error("Failed to parse byte data: " + data + " for JobModel details retrieved from the blob", e);
      throw new SamzaException(e);
    }
  }

}