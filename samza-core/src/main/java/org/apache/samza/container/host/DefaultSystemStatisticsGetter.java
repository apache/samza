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
package org.apache.samza.container.host;

import com.google.common.annotations.VisibleForTesting;


/**
 * An default implementation of {@link SystemStatisticsGetter} that relies on {@link PosixCommandBasedStatisticsGetter},
 * {@link OshiBasedStatisticsGetter}, and {@link LinuxCgroupStatisticsGetter}, implementations.
 */
public class DefaultSystemStatisticsGetter implements SystemStatisticsGetter {
  private final OshiBasedStatisticsGetter oshiBasedStatisticsGetter;
  private final PosixCommandBasedStatisticsGetter posixCommandBasedStatisticsGetter;
  private final LinuxCgroupStatisticsGetter linuxCgroupStatisticsGetter;


  public DefaultSystemStatisticsGetter() {
    this(new OshiBasedStatisticsGetter(), new PosixCommandBasedStatisticsGetter(), new LinuxCgroupStatisticsGetter());
  }

  @VisibleForTesting
  DefaultSystemStatisticsGetter(
      OshiBasedStatisticsGetter oshiBasedStatisticsGetter,
      PosixCommandBasedStatisticsGetter posixCommandBasedStatisticsGetter,
      LinuxCgroupStatisticsGetter linuxCgroupStatisticsGetter
  ) {
    this.oshiBasedStatisticsGetter = oshiBasedStatisticsGetter;
    this.posixCommandBasedStatisticsGetter = posixCommandBasedStatisticsGetter;
    this.linuxCgroupStatisticsGetter = linuxCgroupStatisticsGetter;
  }

  @Override
  public SystemMemoryStatistics getSystemMemoryStatistics() {
    return posixCommandBasedStatisticsGetter.getSystemMemoryStatistics();
  }

  @Override
  public ProcessCPUStatistics getProcessCPUStatistics() {
    return oshiBasedStatisticsGetter.getProcessCPUStatistics();
  }

  @Override
  public LinuxCgroupStatistics getProcessCgroupStatistics() {
    return linuxCgroupStatisticsGetter.getProcessCgroupStatistics();
  }
}
