#!/bin/bash
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# resolve links - $0 may be a softlink
PRG="${0}"

while [ -h "${PRG}" ]; do
  ls=`ls -ld "${PRG}"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "${PRG}"`/"$link"
  fi
done

BASEDIR=`dirname ${PRG}`
BASEDIR=`cd ${BASEDIR}/..;pwd`

KMS_SILENT=${KMS_SILENT:-true}

source ${HADOOP_LIBEXEC_DIR:-${BASEDIR}/libexec}/kms-config.sh

# The Java System property 'kms.http.port' it is not used by Kms,
# it is used in Tomcat's server.xml configuration file
#
print "Using   CATALINA_OPTS:       ${CATALINA_OPTS}"

catalina_opts="-Dkms.home.dir=${KMS_HOME}";
catalina_opts="${catalina_opts} -Dkms.config.dir=${KMS_CONFIG}";
catalina_opts="${catalina_opts} -Dkms.log.dir=${KMS_LOG}";
catalina_opts="${catalina_opts} -Dkms.temp.dir=${KMS_TEMP}";
catalina_opts="${catalina_opts} -Dkms.admin.port=${KMS_ADMIN_PORT}";
catalina_opts="${catalina_opts} -Dkms.http.port=${KMS_HTTP_PORT}";
catalina_opts="${catalina_opts} -Dkms.max.threads=${KMS_MAX_THREADS}";
catalina_opts="${catalina_opts} -Dkms.ssl.keystore.file=${KMS_SSL_KEYSTORE_FILE}";
catalina_opts="${catalina_opts} -Dkms.ssl.keystore.pass=${KMS_SSL_KEYSTORE_PASS}";

print "Adding to CATALINA_OPTS:     ${catalina_opts}"

export CATALINA_OPTS="${CATALINA_OPTS} ${catalina_opts}"

# A bug in catalina.sh script does not use CATALINA_OPTS for stopping the server
#
if [ "${1}" = "stop" ]; then
  export JAVA_OPTS=${CATALINA_OPTS}
fi

exec ${KMS_CATALINA_HOME}/bin/catalina.sh "$@"
