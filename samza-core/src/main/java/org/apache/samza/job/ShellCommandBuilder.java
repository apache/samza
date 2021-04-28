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

package org.apache.samza.job;

import java.io.File;
import java.util.Map;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.samza.config.ShellCommandConfig;


public class ShellCommandBuilder extends CommandBuilder {
  @Override
  public String buildCommand() {
    ShellCommandConfig shellCommandConfig = new ShellCommandConfig(config);
    if (StringUtils.isEmpty(this.commandPath)) {
      return shellCommandConfig.getCommand();
    } else {
      return this.commandPath + File.separator + shellCommandConfig.getCommand();
    }
  }

  @Override
  public Map<String, String> buildEnvironment() {
    ShellCommandConfig shellCommandConfig = new ShellCommandConfig(config);
    ImmutableMap.Builder<String, String> envBuilder = new ImmutableMap.Builder<>();
    envBuilder.put(ShellCommandConfig.ENV_CONTAINER_ID, this.id);
    envBuilder.put(ShellCommandConfig.ENV_COORDINATOR_URL, this.url.toString());
    envBuilder.put(ShellCommandConfig.ENV_JAVA_OPTS, shellCommandConfig.getTaskOpts().orElse(""));
    envBuilder.put(ShellCommandConfig.ENV_ADDITIONAL_CLASSPATH_DIR,
        shellCommandConfig.getAdditionalClasspathDir().orElse(""));
    shellCommandConfig.getJavaHome().ifPresent(javaHome -> envBuilder.put(ShellCommandConfig.ENV_JAVA_HOME, javaHome));
    return envBuilder.build();
  }
}
