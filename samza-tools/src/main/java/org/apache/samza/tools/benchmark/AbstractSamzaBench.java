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

package org.apache.samza.tools.benchmark;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.samza.config.Config;
import org.apache.samza.config.MapConfig;
import org.apache.samza.sql.testutil.ReflectionUtils;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.tools.CommandLineHelper;


/**
 * Base class for the samza benchmark tests
 */

public abstract class AbstractSamzaBench {
  protected static final String OPT_SHORT_PROPERTIES_FILE = "p";
  protected static final String OPT_LONG_PROPERTIES_FILE = "props";
  protected static final String OPT_ARG_PROPERTIES_FILE = "PROPERTIES_FILE";
  protected static final String OPT_DESC_PROPERTIES_FILE = "Path to the properties file.";

  protected static final String OPT_SHORT_NUM_EVENTS = "n";
  protected static final String OPT_LONG_NUM_EVENTS = "numEvents";
  protected static final String OPT_ARG_NUM_EVENTS = "NUMBER_EVENTS";
  protected static final String OPT_DESC_NUM_EVENTS = "Total number of events to consume.";

  protected static final String OPT_SHORT_START_PARTITION = "sp";
  protected static final String OPT_LONG_START_PARTITION = "startPartition";
  protected static final String OPT_ARG_START_PARTITION = "START_PARTITION";
  protected static final String OPT_DESC_START_PARTITION = "Start partition.";

  protected static final String OPT_SHORT_END_PARTITION = "ep";
  protected static final String OPT_LONG_END_PARTITION = "endPartition";
  protected static final String OPT_ARG_END_PARTITION = "END_PARTITION";
  protected static final String OPT_DESC_END_PARTITION = "End partition.";

  protected static final String OPT_SHORT_STREAM = "s";
  protected static final String OPT_LONG_STREAM = "streamId";
  protected static final String OPT_ARG_STREAM = "STREAM_ID";
  protected static final String OPT_DESC_STREAM = "STREAM ID.";
  protected static final String CFG_STREAM_SYSTEM_NAME = "streams.%s.samza.system";
  protected static final String CFG_SYSTEM_FACTORY = "systems.%s.samza.factory";
  protected static final String CFG_PHYSICAL_STREAM_NAME = "streams.%s.samza.physical.name";
  protected final Options options;
  protected final CommandLine cmd;
  protected SystemFactory factory;
  protected Config config;
  protected String systemName;
  protected String physicalStreamName;
  protected int startPartition;
  protected int endPartition;
  protected int totalEvents;
  protected String streamId;

  public AbstractSamzaBench(String scriptName, String args[]) throws ParseException {
    options = new Options();
    options.addOption(
        CommandLineHelper.createOption(OPT_SHORT_PROPERTIES_FILE, OPT_LONG_PROPERTIES_FILE, OPT_ARG_PROPERTIES_FILE,
            true, OPT_DESC_PROPERTIES_FILE));
    options.addOption(
        CommandLineHelper.createOption(OPT_SHORT_NUM_EVENTS, OPT_LONG_NUM_EVENTS, OPT_ARG_NUM_EVENTS, true,
            OPT_DESC_NUM_EVENTS));

    options.addOption(
        CommandLineHelper.createOption(OPT_SHORT_START_PARTITION, OPT_LONG_START_PARTITION, OPT_ARG_START_PARTITION,
            true, OPT_DESC_START_PARTITION));
    options.addOption(
        CommandLineHelper.createOption(OPT_SHORT_END_PARTITION, OPT_LONG_END_PARTITION, OPT_ARG_END_PARTITION, true,
            OPT_DESC_END_PARTITION));
    options.addOption(
        CommandLineHelper.createOption(OPT_SHORT_STREAM, OPT_LONG_STREAM, OPT_ARG_STREAM, true, OPT_DESC_STREAM));

    addOptions(options);

    CommandLineParser parser = new BasicParser();
    try {
      cmd = parser.parse(options, args);
    } catch (Exception e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(String.format("Error: %s.sh", scriptName), options);
      throw e;
    }
  }

  public void start() throws IOException, InterruptedException {
    startPartition = Integer.parseInt(cmd.getOptionValue(OPT_SHORT_START_PARTITION));
    endPartition = Integer.parseInt(cmd.getOptionValue(OPT_SHORT_END_PARTITION));
    totalEvents = Integer.parseInt(cmd.getOptionValue(OPT_SHORT_NUM_EVENTS));
    String propsFile = cmd.getOptionValue(OPT_SHORT_PROPERTIES_FILE);
    streamId = cmd.getOptionValue(OPT_SHORT_STREAM);
    Properties props = new Properties();
    props.load(new FileInputStream(propsFile));
    addMoreSystemConfigs(props);
    config = convertToSamzaConfig(props);
    systemName = config.get(String.format(CFG_STREAM_SYSTEM_NAME, streamId));
    String systemFactory = config.get(String.format(CFG_SYSTEM_FACTORY, systemName));
    physicalStreamName = config.get(String.format(CFG_PHYSICAL_STREAM_NAME, streamId));

    factory = ReflectionUtils.createInstance(systemFactory);
    if (factory == null) {
      throw new RuntimeException("Cannot instantiate systemfactory " + systemFactory);
    }
  }

  /**
   * Derived classes can override this method to add any additional properties needed to create the System
   * @param props Properties to which system configs can be added.
   */
  protected void addMoreSystemConfigs(Properties props) {
  }

  /**
   * Derived classes can override this method to add any additional options that benchmark test may need.
   * @param options Options to which additional command line options can be added.
   */
  protected void addOptions(Options options) {
  }

  Config convertToSamzaConfig(Properties props) {
      Map<String, String> propsValue =
          props.stringPropertyNames().stream().collect(Collectors.toMap(Function.identity(), props::getProperty));
      return new MapConfig(propsValue);
    }
}