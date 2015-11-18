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


package org.apache.samza.autoscaling.deployer;

import joptsimple.OptionSet;

import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.samza.autoscaling.utils.YarnUtil;
import org.apache.samza.config.Config;
import org.apache.samza.config.JobConfig;
import org.apache.samza.container.SamzaContainer;
import org.apache.samza.coordinator.stream.messages.CoordinatorStreamMessage;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemConsumer;
import org.apache.samza.coordinator.stream.CoordinatorStreamSystemFactory;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.job.JobRunner;
import org.apache.samza.job.model.ContainerModel;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.apache.samza.system.SystemStreamPartitionIterator;
import org.apache.samza.util.CommandLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


/**
 * This class is a separate module that runs along side with a job, and handles all config changes submitted to a job after the bootstrap of the job.
 * All config changes are written to the coordinator stream using the @Link{CoordinatorStreamWriter}.
 * The way this class works is that it reads all messages with type "set-config" written to the coordinator stream after
 * the bootstrap of the job, and it handles the messages accordingly.
 * The current configuration changes it handles are
 * 1. changing the number of containers of a job
 * 2. setting the server url for the first time (in order to get the JobModel).
 * In order to use this class the run() method should be called to react to changes,
 * or call the start(), processConfigMessages(), and stop() function instead.
 * Additionally, you have to add the following configurations to the config file:
 * yarn.rm.address=localhost //the ip of the resource manager in yarn
 * yarn.rm.port=8088 //the port of the resource manager http server
 * Additionally, the config manger will periodically poll the coordinator stream to see if there are any new messages.
 * This period is set to 100 ms by default. However, it can be configured by adding the following property to the input config file.
 * configManager.polling.interval=&lt; polling interval &gt;
 */

public class ConfigManager {
  private final CoordinatorStreamSystemConsumer coordinatorStreamConsumer;
  private SystemStreamPartitionIterator coordinatorStreamIterator;
  private static final Logger log = LoggerFactory.getLogger(ConfigManager.class);
  private final long defaultPollingInterval = 100;
  private final long interval;
  private String coordinatorServerURL = null;
  private final String jobName;
  private final int jobID;
  private Config config;

  private YarnUtil yarnUtil;

  private final String rmAddressOpt = "yarn.rm.address";
  private final String rmPortOpt = "yarn.rm.port";
  private final String pollingIntervalOpt = "configManager.polling.interval";
  private static final String SERVER_URL_OPT = "samza.autoscaling.server.url";
  private static final String YARN_CONTAINER_COUNT_OPT = "yarn.container.count";

  public ConfigManager(Config config) {

    //get rm address and port
    if (!config.containsKey(rmAddressOpt) || !config.containsKey(rmPortOpt)) {
      throw new IllegalArgumentException("Missing config: the config file does not contain the rm host or port.");
    }
    String rmAddress = config.get(rmAddressOpt);
    int rmPort = config.getInt(rmPortOpt);

    //get job name and id;
    if (!config.containsKey(JobConfig.JOB_NAME())) {
      throw new IllegalArgumentException("Missing config: the config does not contain the job name");
    }
    jobName = config.get(JobConfig.JOB_NAME());
    jobID = config.getInt(JobConfig.JOB_ID(), 1);

    //set polling interval
    if (config.containsKey(pollingIntervalOpt)) {
      long pollingInterval = config.getLong(pollingIntervalOpt);
      if (pollingInterval <= 0) {
        throw new IllegalArgumentException("polling interval cannot be a negative value");
      }
      this.interval = pollingInterval;
    } else {
      this.interval = defaultPollingInterval;
    }

    this.config = config;
    CoordinatorStreamSystemFactory coordinatorStreamSystemFactory = new CoordinatorStreamSystemFactory();
    this.coordinatorStreamConsumer = coordinatorStreamSystemFactory.getCoordinatorStreamSystemConsumer(config, new MetricsRegistryMap());
    this.yarnUtil = new YarnUtil(rmAddress, rmPort);
  }

  /**
   * This method is an infinite loop that periodically checks if there are any new messages in the job coordinator stream, and reads them if they exist.
   * Then it reacts accordingly based on the configuration that is being set.
   * The method the calls the start() method to initialized the system, runs in a infinite loop, and calls the stop() method at the end to stop the consumer and the system
   */
  public void run() {
    start();
    try {
      while (true) {
        Thread.sleep(interval);
        processConfigMessages();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
      log.warn("Got interrupt in config manager thread, so shutting down");
    } finally {
      log.info("Stopping the config manager");
      stop();
    }
  }

  /**
   * Starts the system by starting the consumer
   */
  public void start() {
    register();
    coordinatorStreamConsumer.start();
    coordinatorStreamIterator = coordinatorStreamConsumer.getStartIterator();
    bootstrap();
  }

  /**
   * stops the consumer making the system ready to stop
   */
  public void stop() {
    coordinatorStreamConsumer.stop();
    coordinatorServerURL = null;
    yarnUtil.stop();
  }

  /**
   * registers the consumer
   */
  private void register() {
    coordinatorStreamConsumer.register();
  }


  /**
   * This function will bootstrap by reading all the unread messages until the moment of calling the function, and therefore find the server url.
   */
  private void bootstrap() {
    List<String> keysToProcess = new LinkedList<>();
    keysToProcess.add(SERVER_URL_OPT);
    processConfigMessages(keysToProcess);
    if (coordinatorServerURL == null) {
      throw new IllegalStateException("coordinator server url is null, while the bootstrap has finished ");
    }
    log.info("Config manager bootstrapped");
  }

  /**
   * skip all the unread messages up to the time this function is called.
   * This method just reads the messages, and it does not react to them or change any configuration of the system.
   */
  private void skipUnreadMessages() {
    processConfigMessages(new LinkedList<String>());
    log.info("Config manager skipped messages");
  }

  /**
   * This function reads all the messages with "set-config" type added to the coordinator stream since the last time the method was invoked
   */
  public void processConfigMessages() {
    List<String> keysToProcess = new LinkedList<>();
    keysToProcess.add(YARN_CONTAINER_COUNT_OPT);
    keysToProcess.add(SERVER_URL_OPT);
    processConfigMessages(keysToProcess);
  }

  /**
   * This function reads all the messages with "set-config" type added to the coordinator stream since the last time the method was invoked
   *
   * @param keysToProcess a list of keys to process. Only messages with these keys will call their handler function,
   *                      and other messages will be skipped. If the list is empty all messages will be skipped.
   */
  @SuppressWarnings("unchecked")
  private void processConfigMessages(List<String> keysToProcess) {
    if (!coordinatorStreamConsumer.hasNewMessages(coordinatorStreamIterator)) {
      return;
    }
    if (keysToProcess == null) {
      throw new IllegalArgumentException("The keys to process list is null");
    }
    for (CoordinatorStreamMessage message : coordinatorStreamConsumer.getUnreadMessages(coordinatorStreamIterator, SetConfig.TYPE)) {
      String key = null;
      try {
        SetConfig setConfigMessage = new SetConfig(message);
        key = setConfigMessage.getKey();
        Map<String, String> valuesMap = (Map<String, String>) setConfigMessage.getMessageMap().get("values");
        String value = null;
        if (valuesMap != null) {
          value = valuesMap.get("value");
        }

        log.debug("Received set-config message with key: " + key + " and value: " + value);

        if (keysToProcess.contains(key)) {
          if (key.equals(YARN_CONTAINER_COUNT_OPT)) {
            handleYarnContainerChange(value);
          } else if (key.equals(SERVER_URL_OPT)) {
            handleServerURLChange(value);
          } else {
            log.info("Setting the " + key + " configuration is currently not supported, skipping the message");
          }
        }

        //TODO: change the handlers to implement a common interface, to make them pluggable
      } catch (Exception e) {
        log.debug("Error in reading a message, skipping message with key " + key);
      }

    }

  }

  /**
   * This method handle setConfig messages that want to change the url of the server the JobCoordinator has brought up.
   *
   * @param newServerURL the new value of the server URL
   */
  private void handleServerURLChange(String newServerURL) {
    this.coordinatorServerURL = newServerURL;
    log.info("Server URL being set to " + newServerURL);
  }

  /**
   * This method handles setConfig messages that want to change the number of containers of a job
   *
   * @param containerCountAsString the new number of containers in a String format
   */
  private void handleYarnContainerChange(String containerCountAsString) throws IOException, YarnException {
    String applicationId = yarnUtil.getRunningAppId(jobName, jobID);

    int containerCount = Integer.valueOf(containerCountAsString);

    //checking the input is valid
    int currentNumTask = getCurrentNumTasks();
    int currentNumContainers = getCurrentNumContainers();
    if (containerCount == currentNumContainers) {
      log.error("The new number of containers is equal to the current number of containers, skipping this message");
      return;
    }
    if (containerCount <= 0) {
      log.error("The number of containers cannot be zero or less, skipping this message");
      return;
    }


    if (containerCount > currentNumTask) {
      log.error("The number of containers cannot be more than the number of task, skipping this message");
      return;
    }


    //killing the current job
    log.info("Killing the current job");
    yarnUtil.killApplication(applicationId);
    //clear the global variables
    coordinatorServerURL = null;


    try {
      //waiting for the job to be killed
      String state = yarnUtil.getApplicationState(applicationId);
      Thread.sleep(1000);
      int countSleep = 1;

      while (!state.equals("KILLED")) {
        state = yarnUtil.getApplicationState(applicationId);
        log.info("Job kill signal sent, but job not killed yet for " + applicationId + ". Sleeping for another 1000ms");
        Thread.sleep(1000);
        countSleep++;
        if (countSleep > 10) {
          throw new IllegalStateException("Job has not been killed after 10 attempts.");
        }
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    log.info("Killed the current job successfully");

    //start the job again
    log.info("Staring the job again");
    skipUnreadMessages();
    JobRunner jobRunner = new JobRunner(config);
    jobRunner.run(false);
  }


  /**
   * This method returns the number of tasks in the job. It works by querying the server, and getting the job model.
   * Then it extracts the number of tasks from the job model
   *
   * @return current number of tasks in the job
   */
  public int getCurrentNumTasks() {
    int currentNumTasks = 0;
    for (ContainerModel containerModel : SamzaContainer.readJobModel(coordinatorServerURL).getContainers().values()) {
      currentNumTasks += containerModel.getTasks().size();
    }
    return currentNumTasks;
  }

  /**
   * This method returns the number of containers in the job. It works by querying the server, and getting the job model.
   * Then it extracts the number of containers from the job model
   *
   * @return current number of containers in the job
   */
  public int getCurrentNumContainers() {
    return SamzaContainer.readJobModel(coordinatorServerURL).getContainers().values().size();
  }


  /**
   * Gets the current value of the server URL that the job coordinator is serving the job model on.
   *
   * @return the current server URL. If null, it means the job has not set the server yet.
   */
  public String getCoordinatorServerURL() {
    return coordinatorServerURL;
  }

  /**
   * Main function for using the Config Manager. The main function starts a Config Manager, and reacts to all messages thereafter
   * In order for this module to run, you have to add the following configurations to the config file:
   * yarn.rm.address=localhost //the ip of the resource manager in yarn
   * yarn.rm.port=8088 //the port of the resource manager http server
   * Additionally, the config manger will periodically poll the coordinator stream to see if there are any new messages.
   * This period is set to 100 ms by default. However, it can be configured by adding the following property to the input config file.
   * configManager.polling.interval= &lt; polling interval &gt;
   * To run the code use the following command:
   * {path to samza deployment}/samza/bin/run-config-manager.sh  --config-factory={config-factory} --config-path={path to config file of a job}
   *
   * @param args input arguments for running ConfigManager.
   */
  public static void main(String[] args) {
    CommandLine cmdline = new CommandLine();
    OptionSet options = cmdline.parser().parse(args);
    Config config = cmdline.loadConfig(options);
    ConfigManager configManager = new ConfigManager(config);
    configManager.run();
  }


}
