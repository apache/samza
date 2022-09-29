#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# samzaJarsFolder includes all the Samza jars (you needs to make sure all the samza jars are there.)
# You can build Samza image by:
#     docker build -t dockerHubAccount/samza:versionNumber .
# Then Samza user can use the Samza image as base image to build their application image.
#

FROM ubuntu:latest

RUN apt-get update -y && apt-get upgrade -y && apt-get install -y openjdk-8-jdk

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

RUN mkdir -p /opt/samza
WORKDIR /opt/samza/
COPY samzaJarsFolder/ /opt/samza/
