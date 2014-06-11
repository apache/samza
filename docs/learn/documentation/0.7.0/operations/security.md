---
layout: page
title: Security
---
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

Samza provides no security. All security is implemented in the stream system, or in the environment that Samza containers run.

### Securing Streaming Systems

Samza does not provide any security at the stream system level. It is up to individual streaming systems to enforce their own security. If a stream system requires usernames and passwords in order to consume from specific streams, these values must be supplied via configuration, and used at the StreamConsumer/StreamConsumerFactory implementation. The same holds true if the streaming system uses SSL certificates or Kerberos. The environment in which Samza runs must provide the appropriate certificate or Kerberos ticket, and the StreamConsumer must be implemented to use these certificates or tickets.

#### Securing Kafka

Kafka provides no security for its topics, and therefore Samza doesn't provide any security when using Kafka topics.

### Securing Samza's Environment

The most important thing to keep in mind when securing an environment that Samza containers run in is that **Samza containers execute arbitrary user code**. They must considered an adversarial application, and the environment must be locked down accordingly.

#### Configuration

Samza reads all configuration at the time a Samza job is started using the run-job.sh script. If configuration contains sensitive information, then care must be taken to provide the JobRunner with the configuration. This means implementing a ConfigFactory that understands the configuration security model, and resolves configuration to Samza's Config object in a secure way.

During the duration of a Samza job's execution, the configuration is kept in memory. The only time configuration is visible is:

1. When configuration is resolved using a ConfigFactory.
2. The configuration is printed to STDOUT when run-job.sh is run.
3. The configuration is written to the logs when a Samza container starts.

If configuration contains sensitive data, then these three points must be secured.

#### Ports

The only port that a Samza container opens by default is an un-secured JMX port that is randomly selected at start time. If this is not desired, JMX can be disabled through configuration. See the [Configuration](configuration.html) page for details.

Users might open ports from inside a Samza container. If this is not desired, then the user that executes the Samza container must have the appropriate permissions revoked, usually using iptables.

#### Logs

Samza container logs contain configuration, and might contain arbitrary sensitive data logged by the user. A secure log directory must be provided to the Samza container.

#### Starting a Samza Job

If operators do not wish to allow Samza containers to be executed by arbitrary users, then the mechanism that Samza containers are deployed must secured. Usually, this means controlling execution of the run-job.sh script. The recommended pattern is to lock down the machines that Samza containers run on, and execute run-job.sh from either a blessed web service or special machine, and only allow access to the service or machine by specific users.

#### Shell Scripts

Please see the [Packaging](packaging.html) section for details on the the shell scripts that Samza uses. Samza containers allow users to execute arbitrary shell commands, so user permissions must be locked down to prevent users from damaging the environment or reading sensitive data.

#### YARN

<!-- TODO make the security page link to the actual YARN security document, when we write it. -->

Samza provides out-of-the-box YARN integration. Take a look at Samza's YARN Security page for details.

## [Kafka &raquo;](kafka.html)
