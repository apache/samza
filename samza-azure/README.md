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

## Samza on Azure

* Provides the ability to run Samza Standalone in the cloud, using Azure.
* Removes dependency from Zookeeper
* All coordination services written using services provided by Azure.

Read [Samza on Azure Design Doc](https://cwiki.apache.org/confluence/display/SAMZA/SEP-7%3A+Samza+on+Azure) to learn more about the implementation details.

### Running Samza with Azure

* Change: job.coordinator.factory = org.apache.samza.AzureJobCoordinatorFactory
* Add Azure Storage Connection String. 
<br /> azure.storage.connect = DefaultEndpointsProtocol=https;AccountName="Insert your account name";AccountKey="Insert your account key"
* Add blob length in bytes => job.coordinator.azure.blob.length
<br /> Default value = 5120000