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
package org.apache.samza.rest.script;

import java.io.FileNotFoundException;
import org.apache.samza.rest.proxy.job.JobInstance;


/**
 * Defines the protocol for getting script paths.
 */
public interface ScriptPathProvider {
  /**
   * @param jobInstance             the job instance which may be used to access the job installation for the script.
   * @param scriptName              the name of the script file. Not the full path.
   * @return                        the full path to the specified script.
   * @throws FileNotFoundException  if the script does not exist.
   */
  String getScriptPath(JobInstance jobInstance, String scriptName)
      throws FileNotFoundException;
}
