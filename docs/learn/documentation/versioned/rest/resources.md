---
layout: page
title: Resources
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

Samza REST can expose any JAX-RS Resource. By default, it ships with a [JobsResource](./resources/jobs.html) which is discussed below. You can implement your own Resources and specify them in the config.

## Implementing a New Resource
Samza REST uses the [Jersey](https://jersey.java.net/) implementation of the [JAX-RS specification](https://jax-rs-spec.java.net/). The Jersey documentation and examples are helpful for creating a new Resource.

In addition, Samza REST imposes the following conventions, which should be followed for all Resources.

### Versioned Paths
By convention, all resources prefix their path with a version number to enable supporting legacy APIs in the future. For example, the base URL for the JobsResource is

	/v1/jobs
If a future version of JobsResource implements a different API, it will use a different version number in the base path.

	/v2/jobs
All resources should be exposed on the latest version so clients can use a common version base for all requests.

### Error Messages
Every error response will include a JSON message body containing a single `message` field describing the problem. For example:

	{
	    "message": "Unrecognized status: null"
	}

### Configuration
There are a few extra steps for Resources that require configuration values.

1. Implement a configuration class that extends [MapConfig](../api/javadocs/org/apache/samza/config/MapConfig.html) with all the constants and accessors for the Resource's configs. The [SamzaRestConfig](javadocs/org/apache/samza/rest/SamzaRestConfig.html) should not be cluttered with Resource configs. See the [JobsResourceConfig](javadocs/org/apache/samza/rest/resources/JobsResourceConfig.html) for an example.
2. Implement or extend a [ResourceFactory](javadocs/org/apache/samza/rest/resources/ResourceFactory.html) which will use the global Samza REST config file to instantiate the MapConfig implementation from step 1 and use it to construct the Resource that requires the config. See the [DefaultResourceFactory](javadocs/org/apache/samza/rest/resources/DefaultResourceFactory.html) for an example.
3. Add all the necessary resource properties to the Samza REST config file. The [SamzaRestApplication](javadocs/org/apache/samza/rest/SamzaRestApplication.html) passes the global config into the configured ResourceFactories to instantiate the Resources with configs.

## Adding a New Resource to the Samza REST Service
Resources are added to the Samza REST Service via config. There are two ways to add a new Resource depending on whether the Resource requires properties from the config file or not. In the former case, the Resource is instantiated once with the config and the instance is registered with the SamzaRestApplication. In the latter case, the Resource *class* is registered and it can be instantiated many times over the lifecycle of the application.

- To add a configured Resource add the fully-qualified class name of the ResourceFactory implementation that instantiates the Resource to the `rest.resource.factory.classes` property in the service config.

* To add a config-less Resource, add the fully-qualified class name of the Resource implementation to the `rest.resource.classes` property in the service config.

For more information on these config properties, see the config table in the [Overview page.](overview.html#configuration)

## [Monitors &raquo;](monitors.html)