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
package org.apache.samza.job.yarn;

/**
 * {@code SamzaContainerLaunchException} indicates an {@link Exception} during container launch.
 * It can wrap another type of {@link Throwable} or {@link Exception}. Ultimately, any exception thrown
 * during container launch should be of this type so it can be handled explicitly.
 */
public class SamzaContainerLaunchException extends Exception {

  private static final long serialVersionUID = -3957939806997013992L;

  public SamzaContainerLaunchException() {
    super();
  }

  public SamzaContainerLaunchException(String s, Throwable t) {
    super(s, t);
  }

  public SamzaContainerLaunchException(String s) {
    super(s);
  }

  public SamzaContainerLaunchException(Throwable t) {
    super(t);
  }
}
