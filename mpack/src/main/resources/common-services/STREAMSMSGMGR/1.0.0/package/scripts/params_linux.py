#!/usr/bin/env python

"""
  HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES

  (c) 2016-2018 Hortonworks, Inc. All rights reserved.

  This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
  Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
  to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
  properly licensed third party, you do not have any rights to this code.

  If this code is provided to you under the terms of the AGPLv3:
  (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
  (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
  (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
    FROM OR RELATED TO THE CODE; AND
  (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
    OR LOSS OR CORRUPTION OF DATA.
"""

import functools
import os

from ambari_commons.credential_store_helper import get_password_from_credential_store
from ambari_commons.os_check import OSCheck
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import format
from resource_management.libraries.functions import get_kinit_path
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions.default import default
from resource_management.libraries.functions.expect import expect
from status_params import *
from resource_management.libraries.functions.get_not_managed_resources import get_not_managed_resources
from resource_management.libraries.functions.is_empty import is_empty
from resource_management.libraries.resources.hdfs_resource import HdfsResource
from resource_management.libraries.script import Script
from resource_management.libraries.functions.get_stack_version import get_stack_version
from resource_management.core.logger import Logger
from resource_management.core.exceptions import Fail
from resource_management.libraries.functions.get_bare_principal import get_bare_principal
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.stack_features import get_stack_feature_version
from resource_management.libraries.functions import StackFeature

# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
original_stack_root = Script.get_stack_root()
stack_name = default("/clusterLevelParams/stack_name", None)
parentDir = os.path.abspath(os.path.join(original_stack_root, os.pardir))
stack_root = os.path.join(parentDir,"smm")

kerberos_security_enabled = config['configurations']['cluster-env']['security_enabled']
kinit_path_local = get_kinit_path(default('/configurations/kerberos-env/executable_search_paths', None))
smokeuser = config['configurations']['cluster-env']['smokeuser']
smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']
smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']

#Check if Ranger is available
ranger_admin_hosts = default("/clusterHostInfo/ranger_admin_hosts", [])
has_ranger_admin = not len(ranger_admin_hosts) == 0

# get the correct version to use for checking stack features
version_for_stack_feature_checks = get_stack_feature_version(config)

# get rewrite stack support
stack_support_rewrite_uri = check_stack_feature('streams_messaging_manager_rewriteuri_filter_support', version_for_stack_feature_checks)
stack_support_allowed_resources = check_stack_feature('streams_messaging_manager_allowed_resources_support', version_for_stack_feature_checks)
stack_support_remove_rootpath = check_stack_feature('streams_messaging_manager_remove_rootpath', version_for_stack_feature_checks)
stack_streams_messaging_manager_support_schema_migrate = check_stack_feature('streams_messaging_manager_support_schema_migrate', version_for_stack_feature_checks)
stack_streams_messaging_manager_support_db_user_creation = check_stack_feature('streams_messaging_manager_support_db_user_creation', version_for_stack_feature_checks)

# When downgrading the 'version' and 'current_version' are both pointing to the downgrade-target version
# downgrade_from_version provides the source-version the downgrade is happening from
downgrade_from_version = default("/commandParams/downgrade_from_version", None)


hostname = config['agentLevelParams']['hostname']

# default streams-messaging-manager parameters
streams_messaging_manager_current = os.path.join(stack_root, "current")
streams_messaging_manager_home = os.path.join(stack_root, "current", "streams-messaging-manager")
streams_messaging_manager_bin = os.path.join(streams_messaging_manager_home, "bin")
streams_messaging_manager_managed_log = os.path.join(streams_messaging_manager_home, "logs")
streams_messaging_manager_managed_libs = os.path.join(streams_messaging_manager_home, "libs")
streams_messaging_manager_managed_bootstrap = os.path.join(streams_messaging_manager_home, "bootstrap")
streams_messaging_manager_script = os.path.join(streams_messaging_manager_bin, "streams-messaging-manager.sh")

conf_dir = os.path.join(streams_messaging_manager_home, "conf")

limits_conf_dir = "/etc/security/limits.d"

streams_messaging_manager_user_nofile_limit = default('/configurations/streams-messaging-manager-env/streams_messaging_manager_user_nofile_limit', 65536)
streams_messaging_manager_user_nproc_limit = default('/configurations/streams-messaging-manager-env/streams_messaging_manager_user_nproc_limit', 65536)

streams_messaging_manager_user = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_user']
streams_messaging_manager_log_dir = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_log_dir']

#log4j params
streams_messaging_manager_log_maxbackupindex = config['configurations']['streams-messaging-manager-log4j']['streams_messaging_manager_log_maxbackupindex']
streams_messaging_manager_log_maxfilesize = config['configurations']['streams-messaging-manager-log4j']['streams_messaging_manager_log_maxfilesize']
streams_messaging_manager_log_template = config['configurations']['streams-messaging-manager-log4j']['content']
streams_messaging_manager_log_template = streams_messaging_manager_log_template.replace('{{streams_messaging_manager_log_dir}}', streams_messaging_manager_log_dir)
streams_messaging_manager_log_template = streams_messaging_manager_log_template.replace('{{streams_messaging_manager_log_maxbackupindex}}', streams_messaging_manager_log_maxbackupindex)
streams_messaging_manager_log_template = streams_messaging_manager_log_template.replace('{{streams_messaging_manager_log_maxfilesize}}', ("%sMB" % streams_messaging_manager_log_maxfilesize))

# This is hardcoded on the streams-messaging-manager bash process lifecycle on which we have no control over
streams_messaging_manager_connecting_pid_file = os.path.join(streams_messaging_manager_managed_log, 'streams-messaging-manager.pid')
streams_messaging_manager_managed_pid_dir = "/var/run/streams-messaging-manager"
streams_messaging_manager_pid_dir = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_pid_dir']

user_group = config['configurations']['cluster-env']['user_group']
jdk64_home=config['ambariLevelParams']['java_home']
streams_messaging_manager_env_sh_template = config['configurations']['streams-messaging-manager-env']['content']
streams_messaging_manager_jaas_conf_template = default("/configurations/streams-messaging-manager_jaas_conf/content", None)
ambari_cluster_name = config['clusterName']
host_sys_prepped = default("/hostLevelParams/host_sys_prepped", False)

#kafka and schema registry properties
kafka_bootstrap_servers = config['configurations']['streams-messaging-manager-common']['kafka.bootstrap.servers']
schema_registry_url = config['configurations']['streams-messaging-manager-common']['schema.registry.url']
kafka_consumer_poll_timeout_ms = config['configurations']['streams-messaging-manager-common']['kafka.consumer.poll.timeout.ms']

#streams messaging manager's db properties
streams_messaging_manager_database_name = str(config['configurations']['streams-messaging-manager-common']['database_name'])
streams_messaging_manager_storage_type = str(config['configurations']['streams-messaging-manager-common']['streams_messaging_manager.storage.type']).lower()
streams_messaging_manager_storage_connector_user = config['configurations']['streams-messaging-manager-common']['streams_messaging_manager.storage.connector.user']
streams_messaging_manager_storage_connector_password = config['configurations']['streams-messaging-manager-common']['streams_messaging_manager.storage.connector.password']
streams_messaging_manager_storage_connector_connectorURI = config['configurations']['streams-messaging-manager-common']['streams_messaging_manager.storage.connector.connectURI']
streams_messaging_manager_storage_query_timeout = config['configurations']['streams-messaging-manager-common']['streams_messaging_manager.storage.query.timeout']

if streams_messaging_manager_storage_type == "postgresql":
  streams_messaging_manager_storage_java_class = "org.postgresql.ds.PGSimpleDataSource"
elif streams_messaging_manager_storage_type == "oracle":
    streams_messaging_manager_storage_java_class = "oracle.jdbc.pool.OracleDataSource"
else:
    streams_messaging_manager_storage_java_class = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource"

#Streams Messaging Manager's AMS properties
ams_kafka_appid = config['configurations']['streams-messaging-manager-common']['ams.kafka.appid']
ams_timeline_metrics_hosts = config['configurations']['streams-messaging-manager-common']['ams.timeline.metrics.hosts']
ams_timeline_metrics_port = config['configurations']['streams-messaging-manager-common']['ams.timeline.metrics.port']

ams_timeline_metrics_protocol = config['configurations']['ams-grafana-ini']['protocol']
ams_timeline_metrics_truststore_password = default("/configurations/ams-ssl-client/ssl.client.truststore.password", "")
ams_timeline_metrics_truststore_path = default("/configurations/ams-ssl-client/ssl.client.truststore.location", "")
ams_timeline_metrics_truststore_type = default("/configurations/ams-ssl-client/ssl.client.truststore.type", "")

metrics_cache_refresh_interval_ms = config['configurations']['streams-messaging-manager-common']['metrics.cache.refresh.interval.ms']
consumer_group_refresh_interval_ms = config['configurations']['streams-messaging-manager-common']['consumer.group.refresh.interval.ms']
inactive_producer_timeout_ms = config['configurations']['streams-messaging-manager-common']['inactive.producer.timeout.ms']
inactive_group_timeout_ms = config['configurations']['streams-messaging-manager-common']['inactive.group.timeout.ms']
metrics_fetcher_threads = config['configurations']['streams-messaging-manager-common']['metrics.fetcher.threads']

#port info
streams_messaging_manager_port = config['configurations']['streams-messaging-manager-common']['port']
streams_messaging_manager_admin_port = config['configurations']['streams-messaging-manager-common']['adminPort']

hostname = config['agentLevelParams']['hostname'].lower()
#Keeping security params for now. May change in future.
if kerberos_security_enabled:
  _hostname_lowercase = config['agentLevelParams']['hostname'].lower()
  smokeuser = config['configurations']['cluster-env']['smokeuser']
  smokeuser_principal = config['configurations']['cluster-env']['smokeuser_principal_name']
  smoke_user_keytab = config['configurations']['cluster-env']['smokeuser_keytab']

  #Below properties are replaced by ambari mentioned in kerberos.json
  _streams_messaging_manager_principal_name = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_principal_name']
  streams_messaging_manager_jaas_principal = _streams_messaging_manager_principal_name.replace('_HOST', _hostname_lowercase)
  streams_messaging_manager_keytab_path = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_keytab']
  _streams_messaging_manager_ui_jaas_principal_name = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_ui_principal_name']
  streams_messaging_manager_ui_jaas_principal = _streams_messaging_manager_ui_jaas_principal_name.replace('_HOST', _hostname_lowercase)
  streams_messaging_manager_ui_keytab_path = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_ui_keytab']

  #Borrowing kafka's kerberos params to insert into smm's jaas_conf.xml
  _kafka_principal_name = config['configurations']['kafka-env']['kafka_principal_name']
  kafka_jaas_principal = _kafka_principal_name.replace('_HOST', _hostname_lowercase)
  kafka_keytab_path = config['configurations']['kafka-env']['kafka_keytab']
  kafka_bare_jaas_principal = get_bare_principal(_kafka_principal_name)
  streams_messaging_manager_kerberos_params = '"-Djavax.security.auth.useSubjectCredsOnly=false -Djava.security.auth.login.config=' + conf_dir + '/streams-messaging-manager_jaas.conf"'

  #streams_messaging_manager_kerberos_principal_name = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_kerberos_principal']
  #streams_messaging_manager_kerberos_keytab = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_kerberos_keytab']
  #streams_messaging_manager_kerberos_name_rules = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_kerberos_name_rules']
  #streams_messaging_manager_kerberos_allowed_resources = config['configurations']['streams-messaging-manager-env']['streams_messaging_manager_allowed_resources']
else:
  streams_messaging_manager_kerberos_params = ''

#If Schema Registry is configured
if schema_registry_url == '':
  schema_registry_configured = False
else:
  schema_registry_configured = True

#if Ranger is installed, include hdf client libs in classpath.
if has_ranger_admin == True:
  hdfs_client_libs = "/usr/hdp/current/hadoop-hdfs-client/*:/usr/hdp/current/hadoop-hdfs-client/lib/*"
else:
  hdfs_client_libs = ""

#SSL related configs
streams_messaging_manager_ssl_enabled = config['configurations']['streams-messaging-manager-ssl-config']['streams_messaging_manager.ssl.isenabled']
streams_messaging_manager_ssl_port = config['configurations']['streams-messaging-manager-common']['streams_messaging_manager.ssl.port']
streams_messaging_manager_ssl_adminPort = config['configurations']['streams-messaging-manager-common']['streams_messaging_manager.ssl.adminPort']
streams_messaging_manager_keyStorePath = config['configurations']['streams-messaging-manager-ssl-config']['streams_messaging_manager.keyStorePath']
streams_messaging_manager_keyStorePassword = config['configurations']['streams-messaging-manager-ssl-config']['streams_messaging_manager.keyStorePassword']
streams_messaging_manager_keyStoreType = config['configurations']['streams-messaging-manager-ssl-config']['streams_messaging_manager.keyStoreType']
streams_messaging_manager_trustStorePath = config['configurations']['streams-messaging-manager-ssl-config']['streams_messaging_manager.trustStorePath']
streams_messaging_manager_trustStorePassword = config['configurations']['streams-messaging-manager-ssl-config']['streams_messaging_manager.trustStorePassword']
streams_messaging_manager_trustStoreType = config['configurations']['streams-messaging-manager-ssl-config']['streams_messaging_manager.trustStoreType']
streams_messaging_manager_validateCerts = config['configurations']['streams-messaging-manager-ssl-config']['streams_messaging_manager.validateCerts']
streams_messaging_manager_validatePeers = config['configurations']['streams-messaging-manager-ssl-config']['streams_messaging_manager.validatePeers']

#SSO related configs
streams_messaging_manager_sso_enabled = config['configurations']['streams-messaging-manager-sso-config']['streams_messaging_manager.sso.enabled']
streams_messaging_manager_authentication_provider_url = config['configurations']['streams-messaging-manager-sso-config']['streams_messaging_manager.authentication.provider.url']
streams_messaging_manager_public_key_pem = config['configurations']['streams-messaging-manager-sso-config']['streams_messaging_manager.public.key.pem']
streams_messaging_manager_authentication_sso_token_validity = config['configurations']['streams-messaging-manager-sso-config']['streams_messaging_manager.token.validity']

# Database jar setup verification
jdk_location = config['ambariLevelParams']['jdk_location']
if 'mysql' == streams_messaging_manager_storage_type:
  jdbc_driver_jar = default("/ambariLevelParams/custom_mysql_jdbc_name", None)
  if jdbc_driver_jar == None:
    Logger.error("Failed to find mysql-java-connector jar. Make sure you followed the steps to register mysql driver")
    Logger.info("Users should register the mysql java driver jar.")
    Logger.info("yum install mysql-connector-java*")
    Logger.info("sudo ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java.jar")
    raise Fail('Unable to establish jdbc connection to your ' + streams_messaging_manager_storage_type + ' instance.')

if 'oracle' == streams_messaging_manager_storage_type:
  jdbc_driver_jar = default("/ambariLevelParams/custom_oracle_jdbc_name", None)
  if jdbc_driver_jar == None:
    Logger.error("Failed to find ojdbc jar. Please download and make sure you followed the steps to register oracle driver")
    Logger.info("Users should register the oracle ojdbc driver jar.")
    Logger.info("Create a symlink e.g. ln -s /usr/share/java/ojdbc6.jar /usr/share/java/ojdbc.jar")
    Logger.info("sudo ambari-server setup --jdbc-db=oracle --jdbc-driver=/usr/share/java/ojdbc.jar")
    raise Fail('Unable to establish ojdbc connection to your ' + streams_messaging_manager_storage_type + ' instance.')

if 'mysql' == streams_messaging_manager_storage_type or 'oracle' == streams_messaging_manager_storage_type:
  connector_curl_source = format("{jdk_location}/{jdbc_driver_jar}")
  connector_download_dir=format("{streams_messaging_manager_home}/libs")
  connector_bootstrap_download_dir=format("{streams_messaging_manager_home}/bootstrap/lib")
  downloaded_custom_connector = format("{tmp_dir}/{jdbc_driver_jar}")

check_db_connection_jar_name = "DBConnectionVerification.jar"
check_db_connection_jar = format("/usr/lib/ambari-agent/{check_db_connection_jar_name}")

streams_messaging_manager_agent_dir = "/var/lib/ambari-agent/data/streams_messaging_manager"

# bootstrap commands
bootstrap_storage_command = os.path.join(streams_messaging_manager_home, "bootstrap", "bootstrap_storage.sh")
bootstrap_storage_run_cmd = format('export JAVA_HOME={jdk64_home}; source {conf_dir}/streams-messaging-manager-env.sh ; {bootstrap_storage_command}')

#RAANGER-kafka config setup
if has_ranger_admin == True:

  #Get cluster name
  cluster_name = config["clusterName"]
  # ranger kafka service/repository name
  repo_name = str(config['clusterName']) + '_kafka'
  repo_name_value = config['configurations']['ranger-kafka-security']['ranger.plugin.kafka.service.name']
  if not is_empty(repo_name_value) and repo_name_value != "{{repo_name}}":
    repo_name = repo_name_value
  #credential file info
  credential_file = format('/etc/ranger/{repo_name}/cred.jceks')

  #ssl xa secure
  version_for_stack_feature_checks = get_stack_feature_version(config)
  stack_supports_ranger_audit_db = check_stack_feature(StackFeature.RANGER_AUDIT_DB_SUPPORT, version_for_stack_feature_checks)
  xml_configurations_supported = check_stack_feature(StackFeature.RANGER_XML_CONFIGURATION, version_for_stack_feature_checks)
  if xml_configurations_supported and stack_supports_ranger_audit_db:
    xa_audit_db_is_enabled = config['configurations']['ranger-kafka-audit']['xasecure.audit.destination.db']
  ssl_keystore_password = config['configurations']['ranger-kafka-policymgr-ssl']['xasecure.policymgr.clientssl.keystore.password'] if xml_configurations_supported else None
  ssl_truststore_password = config['configurations']['ranger-kafka-policymgr-ssl']['xasecure.policymgr.clientssl.truststore.password'] if xml_configurations_supported else None

  ranger_kafka_plugin_audit=config['configurations']['ranger-kafka-audit']
  ranger_kafka_plugin_audit_properties=config['configurations']['ranger-kafka-plugin-properties']
  ranger_kafka_plugin_enabled = ranger_kafka_plugin_audit_properties['ranger-kafka-plugin-enabled']
  ranger_kafka_plugin_policymgr_ssl_properties = config['configurations']['ranger-kafka-policymgr-ssl']
  ranger_kafka_plugin_security_properties=config['configurations']['ranger-kafka-security']
else:
  ranger_kafka_plugin_enabled = "No"


#Kafka Client properties iteration
kafkaAdminClient_dict = {}
kafkaConsumerClient_dict = {}
streams_messaging_manager_common_items = config['configurations']['streams-messaging-manager-common']
for key,value in streams_messaging_manager_common_items.items():
  if len(key.split("-"))>1:
     if key.split("-")[0] == 'kafkaAdminClient':
       property_name = key.split("-")[1].strip()
       kafkaAdminClient_dict[property_name] = value
     if key.split("-")[0] == 'kafkaConsumerClient':
       property_name = key.split("-")[1].strip()
       kafkaConsumerClient_dict[property_name] = value

#JWT cookie params
streams_messaging_manager_sso_expected_jwt_audiences = config['configurations']['streams-messaging-manager-sso-config']['streams_messaging_manager.expected.jwt.audiences']
streams_messaging_manager_sso_jwt_cookie_name = config['configurations']['streams-messaging-manager-sso-config']['streams_messaging_manager.jwt.cookie.name']
streams_messaging_manager_sso_cookie_path = config['configurations']['streams-messaging-manager-sso-config']['streams_messaging_manager.cookie.path']
streams_messaging_manager_sso_cookie_domain = config['configurations']['streams-messaging-manager-sso-config']['streams_messaging_manager.cookie.domain']

