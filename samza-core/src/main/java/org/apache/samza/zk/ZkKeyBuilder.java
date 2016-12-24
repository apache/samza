package org.apache.samza.zk;

public class ZkKeyBuilder {
  private final String pathPrefix;
  public static final String PROCESSORS_PATH = "processors";

  public static final String JOBMODEL_VERSION_PATH = "jobModelVersion";

  public ZkKeyBuilder () {
    this("");
  }
  public ZkKeyBuilder (String pathPrefix) {
    this.pathPrefix = pathPrefix;
  }

  public String getProcessorsPath() {
    return String.format("/%s/%s", pathPrefix, PROCESSORS_PATH);
  }

  public static String parseIdFromPath(String path) {
    if (path != null)
     return path.substring(path.indexOf("processor-"));
    return null;
  }

  public String getJobModelVersionPath() {
    return String.format("/%s/%s", pathPrefix, JOBMODEL_VERSION_PATH);
  }

  public String getJobModelPathPrefix() {
    return String.format("/%s/jobModels", pathPrefix);
  }

  public String getJobModelPath(String jobModelVersion) {
    return String.format("%s/%s", getJobModelPathPrefix(), jobModelVersion);
  }

  public String getJobModelVersionBarrierPrefix() {
    return String.format("/%s/versionBarriers", pathPrefix);
  }

  public String getRootPath() {
    return "/" + pathPrefix;
  }
}
