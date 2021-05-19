package org.apache.samza.storage;

import org.apache.samza.config.Config;
import org.apache.samza.job.model.JobModel;


/**
 * Factory to create instance of {@link StateBackendAdmin}s that needs to be implemented for every
 * state backend
 */
public interface BlobStoreAdminFactory {
  /**
   * Returns an instance of {@link StateBackendAdmin}
   * @param config job configuration
   * @param jobModel Job Model
   */
  StateBackendAdmin getStateBackendAdmin(Config config, JobModel jobModel);
}
