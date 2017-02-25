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
package org.apache.samza.monitor;

/**
 * A Monitor is a class implementing some functionality that should be done every N milliseconds on a YARN RM or NM.
 * Classes specified in the config will have their monitor() method called at a configurable interval.
 * For example, one could implement a Monitor that checks for leaked containers and kills them, ensuring that
 * no leaked container survives on a NodeManager host for more than N ms (where N is the monitor run interval.)
 *
 * Implementations can override .toString() for better logging.
 */
public interface Monitor {

    /**
     * Do the work of the monitor. Because this can be arbitrary behavior up to and including script execution,
     * IPC-related IOExceptions and concurrency-related InterruptedExceptions are caught by the SamzaMonitorService.
     * @throws Exception if there was any problem running the monitor.
     */
    void monitor()
        throws Exception;
}