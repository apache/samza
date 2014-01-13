# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash -x
apt-get -y update
apt-get install -y software-properties-common python-software-properties
add-apt-repository -y ppa:webupd8team/java
apt-get -y update
/bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get -y install oracle-java7-installer oracle-java7-set-default

apt-get -y install git vim wget screen curl

export JAVA_HOME=/usr

su vagrant -c "touch ~/.bashrc"
su vagrant -c "export JAVA_HOME=/usr' >> ~/.bashrc"

cd /tmp
wget http://apache.spinellicreations.com/maven/maven-3/3.1.1/binaries/apache-maven-3.1.1-bin.tar.gz
mkdir -p /opt/apache
cd /opt/apache/
tar -xvf /tmp/apache-maven-3.1.1-bin.tar.gz
export PATH=/opt/apache/apache-maven-3.1.1/bin:$PATH
su vagrant -c "echo 'export PATH=/opt/apache/apache-maven-3.1.1/bin:$PATH' >> ~/.bashrc"

cd /vagrant
su vagrant -c "bin/grid bootstrap"

su vagrant -c "mvn clean package"
su vagrant -c "mkdir -p deploy/samza"
su vagrant -c "tar -xvf ./samza-job-package/target/samza-job-package-0.7.0-dist.tar.gz -C deploy/samza"
su vagrant -c "deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties"
sleep 60
su vagrant -c "deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-parser.properties"
sleep 60
su vagrant -c "deploy/samza/bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://$PWD/deploy/samza/config/wikipedia-stats.properties"
sleep 60
