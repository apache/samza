package org.apache.samza.storage;

import org.apache.samza.config.Config;
import org.apache.samza.job.model.JobModel;


public interface BlobStoreAdminFactory {
  StateBackendAdmin getStateBackendAdmin(Config config, JobModel jobModel);
}
