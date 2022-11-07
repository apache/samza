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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.*;


public class TestDefaultSystemStatisticsGetter {

  @Mock
  private OshiBasedStatisticsGetter oshiBasedStatisticsGetter;
  @Mock
  private PosixCommandBasedStatisticsGetter posixCommandBasedStatisticsGetter;

  private DefaultSystemStatisticsGetter defaultSystemStatisticsGetter;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.defaultSystemStatisticsGetter =
        new DefaultSystemStatisticsGetter(oshiBasedStatisticsGetter, posixCommandBasedStatisticsGetter);
  }

  @Test
  public void testGetSystemMemoryStatistics() {
    defaultSystemStatisticsGetter.getSystemMemoryStatistics();
    verify(posixCommandBasedStatisticsGetter).getSystemMemoryStatistics();
  }

  @Test
  public void testGetProcessCPUStatistics() {
    defaultSystemStatisticsGetter.getProcessCPUStatistics();
    verify(oshiBasedStatisticsGetter).getProcessCPUStatistics();
  }
}
