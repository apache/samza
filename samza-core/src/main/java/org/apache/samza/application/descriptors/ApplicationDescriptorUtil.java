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
package org.apache.samza.application.descriptors;

import org.apache.samza.application.SamzaApplication;
import org.apache.samza.application.StreamApplication;
import org.apache.samza.application.TaskApplication;
import org.apache.samza.config.Config;


/**
 * Util class to help creating {@link ApplicationDescriptorImpl} instance from {@link SamzaApplication} and {@link Config}
 */
public class ApplicationDescriptorUtil {

  private ApplicationDescriptorUtil() {

  }

  /**
   * Create a new instance of {@link ApplicationDescriptorImpl} based on {@link SamzaApplication} and {@link Config}
   *
   * @param app an implementation of {@link SamzaApplication}. The {@code app} has to have a proper fully-qualified class name.
   * @param config the {@link Config} for the application
   * @return the {@link ApplicationDescriptorImpl} instance containing the processing logic and the config
   */
  public static ApplicationDescriptorImpl<? extends ApplicationDescriptor> getAppDescriptor(SamzaApplication app, Config config) {
    if (app instanceof StreamApplication) {
      return new StreamApplicationDescriptorImpl((StreamApplication) app, config);
    }
    if (app instanceof TaskApplication) {
      return new TaskApplicationDescriptorImpl((TaskApplication) app, config);
    }
    throw new IllegalArgumentException(String.format("User application class %s is not supported. Only StreamApplication "
        + "and TaskApplication are supported.", app.getClass().getName()));
  }

}