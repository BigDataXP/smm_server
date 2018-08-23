#!/usr/bin/env bash
# HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
# (c) 2016-2018 Hortonworks, Inc. All rights reserved.
# This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
# Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
# to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
# properly licensed third party, you do not have any rights to this code.
# If this code is provided to you under the terms of the AGPLv3:
# (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
# (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
# LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
# (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
# FROM OR RELATED TO THE CODE; AND
# (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
# DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
# OR LOSS OR CORRUPTION OF DATA.
#

function streamsMessagingManagerStart {
   streamsMessagingManagerStatus -q
   if [[ $? -eq 0 ]]; then
       rm -f ${PID_FILE}
       echo "Starting Streams Messaging Manager"
       APP_CLASS="com.hortonworks.smm.kafka.webservice.KafkaAdminApplication"
       cd ${SMM_HOME}
       nohup ${JAVA} ${SMM_HEAP_OPTS} ${SMM_JVM_PERF_OPTS} ${SMM_KERBEROS_OPTS} ${SMM_DEBUG_OPTS} ${SMM_GC_LOG_OPTS} ${SMM_JMX_OPTS} -cp ${CLASSPATH} "${APP_CLASS}" "server" "$@" 2>>"${ERR_FILE}" 1>>"${OUT_FILE}" &
       cd - &> /dev/null
       echo $! > ${PID_FILE}
       echo $(streamsMessagingManagerStatus)
   else
       echo "Streams Messaging Manager already running with PID: ${PID}"
   fi
}

function streamsMessagingManagerStop {
   streamsMessagingManagerStatus -q
   if [[ $? -eq 1 ]]; then
       echo "Stopping Streams Messaging Manager [${PID}] "
       kill -s KILL ${PID} 1>>"${OUT_FILE}" 2>>"${ERR_FILE}"
   else
       echo "Streams Messaging Manager not running"
   fi
}

function streamsMessagingManagerStatus {
   local verbose=1
   while true; do
        case ${1} in
            -q) verbose=0; shift;;
            *) break;;
        esac
   done

   getPID
   if [[ $? -eq 1 ]] || [[ ${PID} -eq 0 ]]; then
     [[ "${verbose}" -eq 1 ]] && echo "Streams Messaging Manager not running."
     return 0
   fi

   ps -p ${PID} > /dev/null
   if [[ $? -eq 0 ]]; then
     [[ "${verbose}" -eq 1 ]] && echo "Streams Messaging Manager running with PID=${PID}."
     return 1
   else
     [[ "${verbose}" -eq 1 ]] && echo "Streams Messaging Manager not running."
     return 0
   fi
}

# Removes the PID file if Streams Messaging Manager is not running
function streamsMessagingManagerClean {
   streamsMessagingManagerStatus -q
   if [[ $? -eq 0 ]]; then
     rm -f ${PID_FILE} ${OUT_FILE} ${ERR_FILE}
     echo "Removed the ${PID_FILE}, ${OUT_FILE} and ${ERR_FILE} files."
   else
     echo "Can't clean files. Streams Messaging Manager running with PID=${PID}."
   fi
}

# Returns 0 if the Knox is running and sets the $PID variable.
function getPID {
   if [ ! -d $PID_DIR ]; then
      printf "Can't find pid dir.\n"
      exit 1
   fi
   if [ ! -f $PID_FILE ]; then
     PID=0
     return 1
   fi

   PID="$(<$PID_FILE)"
   return 0
}

# Home Dir
base_dir=$(dirname $0)/..
cd $base_dir && base_dir=`pwd` && cd - &> /dev/null

# App name
APP_NAME=streams-messaging-manager

#if SMM_DIR is not set its a dev env.
if [ "x$SMM_DIR" != "x" ]; then
    SMM_HOME=$SMM_DIR/$APP_NAME
else
    SMM_HOME=$base_dir
    PID_DIR=$SMM_HOME/logs
    LOG_DIR=$SMM_HOME/logs
    mkdir -p $LOG_DIR
fi

# SMM env script
. ${SMM_HOME}/conf/streams-messaging-manager-env.sh

PID=0

[ -z $PID_DIR ] && PID_DIR="/var/run/$APP_NAME"
[ -z $LOG_DIR ] && LOG_DIR="/var/log/$APP_NAME"

# Name of PID, OUT and ERR files
PID_FILE="${PID_DIR}/${APP_NAME}.pid"
OUT_FILE="${LOG_DIR}/${APP_NAME}.log"
ERR_FILE="${LOG_DIR}/${APP_NAME}.err"

# add ranger, hadoop-hdfs-client conf to classpath
CLASSPATH=${CLASSPATH}:/etc/streams-messaging-manager/conf:/etc/hadoop/conf

# classpath addition for release
for file in ${SMM_HOME}/libs/*.jar;
do
    CLASSPATH=${CLASSPATH}:${file}
done

# Which java to use
if [[ -z "${JAVA_HOME}" ]]; then
  JAVA="java"
else
  JAVA="${JAVA_HOME}/bin/java"
fi

if [[ "x${SMM_HEAP_OPTS}" = "x" ]]; then
    export SMM_HEAP_OPTS="-Xmx2G -Xms2G"
fi

# JVM performance options
if [[ -z "${SMM_JVM_PERF_OPTS}" ]]; then
  SMM_JVM_PERF_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=20 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ExplicitGCInvokesConcurrent -Djava.awt.headless=true"
fi

#JAAS config file params
if [ -z "$SMM_KERBEROS_OPTS" ]; then
    SMM_KERBEROS_OPTS=""
fi

# JMX settings
if [ -z "$SMM_JMX_OPTS" ]; then
  SMM_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false "
fi

# JMX port to use
if [  $JMX_PORT ]; then
  SMM_JMX_OPTS="$SMM_JMX_OPTS -Dcom.sun.management.jmxremote.port=$JMX_PORT "
fi

# GC options
GC_LOG_FILE_NAME='smm-gc.log'
if [ -z "$SMM_GC_LOG_OPTS" ]; then
  JAVA_MAJOR_VERSION=$($JAVA -version 2>&1 | sed -E -n 's/.* version "([0-9]*).*$/\1/p')
  if [[ "$JAVA_MAJOR_VERSION" -ge "9" ]] ; then
    SMM_GC_LOG_OPTS="-Xlog:gc*:file=$LOG_DIR/$GC_LOG_FILE_NAME:time,tags:filecount=10,filesize=102400"
  else
    SMM_GC_LOG_OPTS="-Xloggc:$LOG_DIR/$GC_LOG_FILE_NAME -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=100M"
  fi
fi

option="$1"
shift
case "${option}" in
  start)
     conf="$SMM_HOME/conf/streams-messaging-manager.yaml"
     if [[ $# -eq 1 ]]; then conf="${1}"; fi
     streamsMessagingManagerStart "${conf}"
     ;;
  stop)
     streamsMessagingManagerStop
     ;;
  status)
     streamsMessagingManagerStatus
     ;;
  clean)
     streamsMessagingManagerClean
     ;;
  *)
     echo "Usage: $0 {start|stop|status|clean}"
     ;;
esac
