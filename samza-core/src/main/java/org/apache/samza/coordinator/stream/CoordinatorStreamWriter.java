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

package org.apache.samza.coordinator.stream;

import joptsimple.OptionSet;
import org.apache.samza.config.Config;
import org.apache.samza.coordinator.stream.messages.SetConfig;
import org.apache.samza.metrics.MetricsRegistryMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class writes control messages to the CoordinatorStream.
 * To use this class it, first, it should be initialized by the start() method,
 * and then use the sendMessage() function to send all the control messages needed.
 * Finally, the stop() method should be called.
 * The control messages are in the format of a (type, key, value) where:
 * type: defines the kind of message of the control message from the set {set-config}.
 * key: defines a key to associate with the value. This can be null as well for messages with no value
 * value: defines the value being sent along with the message. This can be null as well for messages with no value.
 */
public class CoordinatorStreamWriter {

  private static final Logger log = LoggerFactory.getLogger(CoordinatorStreamWriter.class);
  public final static String SOURCE = "coordinator-stream-writer";
  public static final String SET_CONFIG_TYPE = SetConfig.TYPE;

  private CoordinatorStreamSystemProducer coordinatorStreamSystemProducer;


  public CoordinatorStreamWriter(Config config) {
    coordinatorStreamSystemProducer = new CoordinatorStreamSystemFactory().getCoordinatorStreamSystemProducer(config, new MetricsRegistryMap());
  }

  /**
   * This method initializes the writer by starting a coordinator stream producer.
   */
  public void start() {
    coordinatorStreamSystemProducer.register(CoordinatorStreamWriter.SOURCE);
    coordinatorStreamSystemProducer.start();
    log.info("Started coordinator stream writer.");
  }

  /**
   * This method stops the writer and closes the coordinator stream producer
   */
  public void stop() {
    log.info("Stopping the coordinator stream producer.");
    coordinatorStreamSystemProducer.stop();
  }

  /**
   * This method sends a message to the coordinator stream. This creates a message containing (type,key,value).
   * For example if you want to set the number of yarn containers to 3, you would use
   * ("set-config", "yarn.container.count", "3").
   *
   * @param type  defines the kind of message of the control message from the set {"set-config"}.
   * @param key   defines a key to associate with the value.  This can be null for messages with no key or value.
   * @param value defines the value being sent along with the message. This can be null for messages with no value.
   */
  public void sendMessage(String type, String key, String value) {
    //TODO: validate keys and values
    if (type.equals(SET_CONFIG_TYPE)) {
      sendSetConfigMessage(key, value);
    } else {
      throw new IllegalArgumentException("Type is invalid. The possible values are {" + SET_CONFIG_TYPE + "}");
    }
  }

  /**
   * This method sends message of type "set-config" to the coordinator stream
   *
   * @param key   defines the name of the configuration being set. For example, for setting the number of yarn containers,
   *              the key is "yarn.container.count"
   * @param value defines the value associated with the key. For example, if the key is "yarn.container.count" the value
   *              is the new number of containers.
   */
  private void sendSetConfigMessage(String key, String value) {
    log.info("sent SetConfig message with key = " + key + " and value = " + value);
    coordinatorStreamSystemProducer.send(new SetConfig(CoordinatorStreamWriter.SOURCE, key, value));
  }

  /**
   * Main function for using the CoordinatorStreamWriter. The main function starts a CoordinatorStreamWriter
   * and sends control messages.
   * To run the code use the following command:
   * {path to samza deployment}/samza/bin/run-coordinator-stream-writer.sh  --config-factory={config-factory} --config-path={path to config file of a job} --type={type of the message} --key={[optional] key of the message} --value={[optional] value of the message}
   *
   * @param args input arguments for running the writer. These arguments are:
   *             "config-factory" = The config file factory
   *             "config-path" = The path to config file of a job
   *             "type" = type of the message being written
   *             "key" = [optional] key of the message being written
   *             "value" = [optional] value of the message being written
   */
  public static void main(String[] args) {
    CoordinatorStreamWriterCommandLine cmdline = new CoordinatorStreamWriterCommandLine();
    OptionSet options = cmdline.parser().parse(args);
    Config config = cmdline.loadConfig(options);
    String type = cmdline.loadType(options);
    String key = cmdline.loadKey(options);
    String value = cmdline.loadValue(options);

    CoordinatorStreamWriter writer = new CoordinatorStreamWriter(config);
    writer.start();
    writer.sendMessage(type, key, value);
    writer.stop();
  }

}

