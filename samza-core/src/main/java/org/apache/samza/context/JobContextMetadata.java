package org.apache.samza.context;

import org.apache.samza.job.model.JobModel;
import org.apache.samza.system.StreamMetadataCache;

import java.util.HashMap;
import java.util.Map;


public class JobContextMetadata {
    private final JobModel jobModel;
    private final StreamMetadataCache streamMetadataCache;
    private final Map<String, Object> objectRegistry = new HashMap<>();

    public JobContextMetadata(JobModel jobModel, StreamMetadataCache streamMetadataCache) {
        this.jobModel = jobModel;
        this.streamMetadataCache = streamMetadataCache;
    }

    public void registerObject(String name, Object value) {
        this.objectRegistry.put(name, value);
    }

    public Object fetchObject(String name) {
        return this.objectRegistry.get(name);
    }

    public JobModel getJobModel() {
        return this.jobModel;
    }

    public StreamMetadataCache getStreamMetadataCache() {
        return this.streamMetadataCache;
    }
}
