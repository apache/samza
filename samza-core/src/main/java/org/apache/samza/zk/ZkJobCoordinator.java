package org.apache.samza.zk;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.samza.SamzaException;
import org.apache.samza.config.Config;
import org.apache.samza.config.JavaSystemConfig;
import org.apache.samza.config.MapConfig;
import org.apache.samza.coordinator.JobCoordinator;
import org.apache.samza.coordinator.JobModelManager;
import org.apache.samza.coordinator.JobModelManager$;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.job.model.JobModel;
import org.apache.samza.processor.SamzaContainerController;
import org.apache.samza.system.StreamMetadataCache;
import org.apache.samza.system.SystemAdmin;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.util.SystemClock;
import org.apache.samza.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * JobCoordinator for stand alone processor managed via Zookeeper.
 */
public class ZkJobCoordinator implements JobCoordinator, ZkListener {
  private static final Logger log = LoggerFactory.getLogger(ZkJobCoordinator.class);

  private final ZkUtils zkUtils;
  private final int processorId;
  private final ZkController zkController;
  private final SamzaContainerController containerController;

  private final BarrierForVersionUpgrade barrier;


  /////////////////////////////////////////
  private JobModel newJobModel;
  private String newJobModelVersion;  // version published in ZK (by the leader)
  private Config config;
  private ZkKeyBuilder keyBuilder;
  private final ScheduleAfterDebounceTime debounceTimer;
  JobModelManager jobModelManager;

  public ZkJobCoordinator(int processorId, Config config, ScheduleAfterDebounceTime debounceTimer, ZkUtils zkUtils, SamzaContainerController containerController) {
    this.zkUtils = zkUtils;
    this.keyBuilder = zkUtils.getKeyBuilder();
    this.debounceTimer = debounceTimer;
    this.processorId = processorId;
    this.containerController = containerController;
    this.zkController = new ZkControllerImpl(String.valueOf(processorId), zkUtils, debounceTimer, this);
    this.config = config;


    barrier = new ZkBarrierForVersionUpgrade(zkUtils, debounceTimer); //should not have any state in it



    // TEMP for model generation
    //////////////////////////////// NEEDS TO BE REPLACED //////////////////////////////////////
    JavaSystemConfig systemConfig = new JavaSystemConfig(this.config);
    Map<String, SystemAdmin> systemAdmins = new HashMap<>();
    for (String systemName: systemConfig.getSystemNames()) {
      String systemFactoryClassName = systemConfig.getSystemFactory(systemName);
      if (systemFactoryClassName == null) {
        log.error(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
        throw new SamzaException(String.format("A stream uses system %s, which is missing from the configuration.", systemName));
      }
      SystemFactory systemFactory = Util.getObj(systemFactoryClassName);
      systemAdmins.put(systemName, systemFactory.getAdmin(systemName, this.config));
    }

    StreamMetadataCache
        streamMetadataCache = new StreamMetadataCache(Util.<String, SystemAdmin>javaMapAsScalaMap(systemAdmins), 5000, SystemClock
        .instance());

    jobModelManager = JobModelManager$.MODULE$.getJobCoordinator(this.config, null, null, streamMetadataCache, null);

    ////////////////////////////////////////////////////////////////////////////////////////////
  }

  @Override
  public void start() {
    zkController.register();
  }

  public void cleanupZk() {
    zkUtils.deleteRoot();
  }

  @Override
  public void stop() {
    zkController.stop();
  }

  @Override
  public int getProcessorId()
  {
    return processorId;
  }

  @Override
  public JobModel getJobModel() {
    return newJobModel;
  }

  //////////////////////////////////////////////// LEADER stuff ///////////////////////////
  @Override
  public void onBecomeLeader() {
    log.info("ZkJobCoordinator::onBecomeLeader - I become the leader!");
    //zkController.listenToProcessorLiveness();
    // Reset debounce Timer

    // generate JobProcess
    generateNewJobModel();
  }

  private void generateNewJobModel() {
    // get the current list of processors
    List<String> currentProcessors = zkUtils.getActiveProcessors();

    // get the current version
    String currentJMVersion  = zkUtils.getJobModelVersion();
    String nextJMVersion;
    if(currentJMVersion == null)
      nextJMVersion = "1";
    else
      nextJMVersion = Integer.toString(Integer.valueOf(currentJMVersion) + 1);
    log.info("pid=" + processorId + "generating new model. Version = " + nextJMVersion);

    Map<String, String> configMap = new HashMap<>();
    Map<Integer, ContainerModel> containers = new HashMap<>();
    MapConfig config = new MapConfig(configMap);
    //JobModel jobModel = new JobModel(config, containers);
    JobModel jobModel = jobModelManager.jobModel();

    log.info("pid=" + processorId + "Generated jobModel: " + jobModel);

    // publish the new version
    zkUtils.publishNewJobModel(nextJMVersion, jobModel);
    log.info("pid=" + processorId + "published new JobModel ver=" + nextJMVersion + ";jm=" + jobModel);

    // start the barrier for the job model update
    barrier.leaderStartBarrier(nextJMVersion, currentProcessors);

    // publish new JobModel version
    zkUtils.publishNewJobModelVersion(currentJMVersion, nextJMVersion);
    log.info("pid=" + processorId + "published new JobModel ver=" + nextJMVersion);
  }

   //////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public void onProcessorChange(List<String> processorIds) {
    // Reset debounce Timer
    log.info("ZkJobCoordinator::onProcessorChange - Processors changed! List: " + Arrays.toString(processorIds.toArray()));
    generateNewJobModel();
  }

  @Override
  public void onNewJobModelAvailable(final String version) {
    newJobModelVersion = version;
    log.info("pid=" + processorId + "new JobModel available");
    // stop current work
    containerController.stopContainer();
    log.info("pid=" + processorId + "new JobModel available.Container stopped.");
    // get the new job model
    newJobModel = zkUtils.getJobModel(version);
    log.info("pid=" + processorId + "new JobModel available. ver=" + version + "; jm = " + newJobModel);


    String currentPath = zkUtils.getEphemeralPath();

    String zkProcessorId = keyBuilder.parseIdFromPath(currentPath);

    // update ZK and wait for all the processors to get this new version
    barrier.waitForBarrier(version, String.valueOf(zkProcessorId), new Runnable() {
      @Override
      public void run() {
        onNewJobModelConfirmed(version);
      }
    });
  }

  @Override
  public void onNewJobModelConfirmed(String version) {
    log.info("pid=" + processorId + "new version " + version + " of the job model got confirmed");
    // get the new Model
    // ?????
    JobModel jobModel = getJobModel();
    log.info("pid=" + processorId + "got the new job model =" + jobModel);

    containerController.startContainer(
        jobModel.getContainers().get(processorId),
        jobModel.getConfig(),
        jobModel.maxChangeLogStreamPartitions);
  }
}
