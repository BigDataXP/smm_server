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

# Resolve links - $0 may be a softlink
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

BOOTSTRAP_DIR=`dirname ${PRG}`
CONFIG_FILE_PATH=${BOOTSTRAP_DIR}/../conf/streams-messaging-manager.yaml
MYSQL_JAR_URL_PATH=https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-5.1.40.zip
SCRIPT_ROOT_DIR="${BOOTSTRAP_DIR}/sql"

# Which java to use
if [ -z "${JAVA_HOME}" ]; then
  JAVA="java"
else
  JAVA="${JAVA_HOME}/bin/java"
fi

TABLE_INITIALIZER_MAIN_CLASS=com.hortonworks.registries.storage.tool.sql.TablesInitializer
for file in "${BOOTSTRAP_DIR}"/lib/*.jar;
do
    CLASSPATH="$CLASSPATH":"$file"
done

execute() {
    echo "Using Configuration file: ${CONFIG_FILE_PATH}"
    ${JAVA} -Dbootstrap.dir=$BOOTSTRAP_DIR  -cp ${CLASSPATH} ${TABLE_INITIALIZER_MAIN_CLASS} -m ${MYSQL_JAR_URL_PATH} -c ${CONFIG_FILE_PATH} -s ${SCRIPT_ROOT_DIR} --${1}
}

printUsage() {
    cat <<-EOF
USAGE: $0 [create|migrate|info|validate|drop|drop-create|repair|check-connection]
   create           : Creates the tables. The target database should be empty
   migrate          : Migrates the database to the latest version or creates the tables if the database is empty. Use "info" to see the current version and the pending migrations
   info             : Shows the list of migrations applied and the pending migration waiting to be applied on the target database
   validate         : Checks if the all the migrations haven been applied on the target database
   drop             : Drops all the tables in the target database
   drop-create      : Drops and recreates all the tables in the target database.
   repair           : Repairs the DATABASE_CHANGE_LOG table which is used to track all the migrations on the target database.
                      This involves removing entries for the failed migrations and update the checksum of migrations already applied on the target databsase.
   check-connection : Checks if a connection can be sucessfully obtained for the target database
EOF
}

if [ $# -gt 1 ]
then
    echo "More than one argument specified, please use only one of the below options"
    printUsage
    exit 1
fi

opt="$1"

case "${opt}" in
create | drop | migrate | info | validate | repair | check-connection )
    execute "${opt}"
    ;;
drop-create )
    execute "drop" && execute "create"
    ;;
*)
    printUsage
    exit 1
    ;;
esac