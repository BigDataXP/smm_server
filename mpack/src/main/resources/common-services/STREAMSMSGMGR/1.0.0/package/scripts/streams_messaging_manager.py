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

import os.path
import traceback
import collections
import os

from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.resources.properties_file import PropertiesFile
from resource_management.libraries.resources.template_config import TemplateConfig
from resource_management.core.resources.system import Directory, Execute, File, Link
from resource_management.core.source import StaticFile, Template, InlineTemplate, DownloadSource
from resource_management.libraries.functions import format
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions import Direction
from resource_management.libraries.functions.get_user_call_output import get_user_call_output
from resource_management.core.exceptions import ExecutionFailed
from resource_management.core.logger import Logger
from resource_management.core.exceptions import Fail
#from ambari_commons.os_family_impl import OsFamilyFuncImpl, OsFamilyImpl
from resource_management.libraries.resources.xml_config import XmlConfig
from resource_management.core.shell import as_sudo
from resource_management.core import sudo

import urllib2, time, json

#@OsFamilyFuncImpl(os_family = OsFamilyImpl.DEFAULT)
def streams_messaging_manager(env, upgrade_type=None):
    import params
    ensure_base_directories()
    # Logger.info(format("Effective stack version: {effective_version}"))

    File(format("{conf_dir}/streams-messaging-manager-env.sh"),
         owner=params.streams_messaging_manager_user,
         content=InlineTemplate(params.streams_messaging_manager_env_sh_template)
         )

    # On some OS this folder could be not exists, so we will create it before pushing there files
    Directory(params.limits_conf_dir,
              create_parents=True,
              owner='root',
              group='root'
              )

    File(os.path.join(params.limits_conf_dir, 'streams-messaging-manager.conf'),
         owner='root',
         group='root',
         mode=0644,
         content=Template("streams-messaging-manager.conf.j2")
         )

    File(format("{conf_dir}/streams-messaging-manager.yaml"),
         content=Template("streams-messaging-manager.yaml.j2"),
         owner=params.streams_messaging_manager_user,
         group=params.user_group,
         mode=0644
         )

    # Keeping security params for now. May change in future. Later will add streams_messaging_manager_jaas.conf
    if params.kerberos_security_enabled:
        if params.streams_messaging_manager_jaas_conf_template:
            File(format("{conf_dir}/streams-messaging-manager_jaas.conf"),
                 owner=params.streams_messaging_manager_user,
                 content=InlineTemplate(params.streams_messaging_manager_jaas_conf_template))
        else:
            TemplateConfig(format("{conf_dir}/streams-messaging-manager_jaas.conf"),
                           owner=params.streams_messaging_manager_user)

    #if not os.path.islink(params.streams_messaging_manager_managed_log_dir):
    #    Link(params.streams_messaging_manager_managed_log_dir,
    #         to=params.streams_messaging_manager_log_dir)

    #Link /etc/streams-messaging-manager/conf
    if not sudo.path_isdir("/etc/streams-messaging-manager/conf"):
      Execute("ln -s " + params.conf_dir + " /etc/streams-messaging-manager", user="root")

    download_database_connector_if_needed()

    # RANGER-kafka config setup
    if params.has_ranger_admin == True:
      if (params.ranger_kafka_plugin_enabled.lower() == "yes"):
        if params.ranger_kafka_plugin_audit:
          XmlConfig("ranger-kafka-audit.xml",
                    conf_dir=params.conf_dir,
                    configurations=params.config['configurations']['ranger-kafka-audit'],
                    configuration_attributes=params.config['configuration_attributes']['ranger-kafka-audit'],
                    owner=params.streams_messaging_manager_user,
                    group=params.user_group,
                    mode=0744)

        if params.ranger_kafka_plugin_audit_properties:
          XmlConfig("ranger-kafka-plugin-properties.xml",
                    conf_dir=params.conf_dir,
                    configurations=params.config['configurations']['ranger-kafka-plugin-properties'],
                    configuration_attributes=params.config['configuration_attributes']['ranger-kafka-plugin-properties'],
                    owner=params.streams_messaging_manager_user,
                    group=params.user_group,
                    mode=0744)

        if params.ranger_kafka_plugin_policymgr_ssl_properties:
          XmlConfig("ranger-policymgr-ssl.xml",
                    conf_dir=params.conf_dir,
                    configurations=params.config['configurations']['ranger-kafka-policymgr-ssl'],
                    configuration_attributes=params.config['configuration_attributes']['ranger-kafka-policymgr-ssl'],
                    owner=params.streams_messaging_manager_user,
                    group=params.user_group,
                    mode=0744)

        if params.ranger_kafka_plugin_security_properties:
          XmlConfig("ranger-kafka-security.xml",
                    conf_dir=params.conf_dir,
                    configurations=params.config['configurations']['ranger-kafka-security'],
                    configuration_attributes=params.config['configuration_attributes']['ranger-kafka-security'],
                    owner=params.streams_messaging_manager_user,
                    group=params.user_group,
                    mode=0744)

      copy_ranger_kafka_jars()


def ensure_base_directories():
    import params
    import status_params
    Directory(
        [params.streams_messaging_manager_log_dir, status_params.streams_messaging_manager_pid_dir, params.conf_dir, params.streams_messaging_manager_agent_dir, params.streams_messaging_manager_managed_libs, params.streams_messaging_manager_managed_bootstrap],
        mode=0755,
        cd_access='a',
        owner=params.streams_messaging_manager_user,
        group=params.user_group,
        create_parents=True,
        recursive_ownership=True,
        )

def download_database_connector_if_needed():
  """
  Downloads the database connector to use when connecting to the metadata storage
  """
  import params
  if params.streams_messaging_manager_storage_type != 'mysql' and params.streams_messaging_manager_storage_type != 'oracle':
      # In any other case than oracle and mysql, e.g. postgres, just return.
      return

  if params.jdbc_driver_jar == None:
      if "mysql" in params.streams_messaging_manager_storage_type:
          Logger.error("Failed to find mysql-java-connector jar. Make sure you followed the steps to register mysql driver")
          Logger.info("Users should register the mysql java driver jar.")
          Logger.info("yum install mysql-connector-java*")
          Logger.info("sudo ambari-server setup --jdbc-db=mysql --jdbc-driver=/usr/share/java/mysql-connector-java.jar")
          raise Fail('Unable to establish jdbc connection to your ' + params.streams_messaging_manager_storage_type + ' instance.')
      if "oracle" in params.streams_messaging_manager_storage_type:
          Logger.error("Failed to find ojdbc jar. Please download and make sure you followed the steps to register oracle jdbc driver")
          Logger.info("Users should register the oracle ojdbc driver jar.")
          Logger.info("Create a symlink e.g. ln -s /usr/share/java/ojdbc6.jar /usr/share/java/ojdbc.jar")
          Logger.info("sudo ambari-server setup --jdbc-db=oracle --jdbc-driver=/usr/share/java/ojdbc.jar")
          raise Fail('Unable to establish jdbc connection to your ' + params.streams_messaging_manager_storage_type + ' instance.')

  File(params.check_db_connection_jar,
       content = DownloadSource(format("{jdk_location}/{check_db_connection_jar_name}")))

  target_jar_with_directory = params.connector_download_dir + os.path.sep + params.jdbc_driver_jar
  target_jar_bootstrap_dir = params.connector_bootstrap_download_dir + os.path.sep + params.jdbc_driver_jar

  if not os.path.exists(target_jar_with_directory):
      File(params.downloaded_custom_connector,
           content=DownloadSource(params.connector_curl_source))

      Execute(('cp', '--remove-destination', params.downloaded_custom_connector, target_jar_with_directory),
              path=["/bin", "/usr/bin/"],
              sudo=True)

      File(target_jar_with_directory, owner="root",
           group=params.user_group)

  if not os.path.exists(target_jar_bootstrap_dir):
      File(params.downloaded_custom_connector,
         content=DownloadSource(params.connector_curl_source))

      Execute(('cp', '--remove-destination', params.downloaded_custom_connector, target_jar_bootstrap_dir),
              path=["/bin", "/usr/bin/"],
              sudo=True)

      File(target_jar_with_directory, owner="root",
           group=params.user_group)

def wait_until_server_starts():
    import params
    #Change below api based on Streams Messaging Manager's REST Admin Server api
    if params.streams_messaging_manager_ssl_enabled:
      streams_messaging_manager_api = format("https://{params.hostname}:{params.streams_messaging_manager_ssl_port}/api/v1/admin/brokers")
    else:
      streams_messaging_manager_api = format("http://{params.hostname}:{params.streams_messaging_manager_port}/api/v1/admin/brokers")
    Logger.info(streams_messaging_manager_api)
    max_retries = 8
    success = False
    curl_connection_timeout = '5'
    for num in range(0, max_retries):
      try:
        Logger.info(format("Making http requests to {streams_messaging_manager_api}"))
        response = urllib2.urlopen(streams_messaging_manager_api)
        api_response = response.read()
        response_code = response.getcode()
        Logger.info(format("Streams Messaging Manager's REST Admin Server response http status {response}"))
        if response.getcode() != 200:
          Logger.error(format("Failed to fetch response for {streams_messaging_manager_api}"))
          show_logs(params.streams_messaging_manager_log_dir, params.streams_messaging_manager_user)
          raise
        else:
          success = True
          Logger.info(format("Successfully made a API request to Streams Messaging Manager's REST Admin Server. {api_response}"))
          break
      except (urllib2.URLError, ExecutionFailed) as e:
        Logger.error(format("Failed to make API request to Streams Messaging Manager's Rest Admin server at {streams_messaging_manager_api},retrying.. {num} out of {max_retries}"))
        time.sleep(num * 5) #exponential back-off
        continue

    if success != True:
      Logger.error(format("Failed to make API request to Streams Messaging Manager's Rest Admin server at {streams_messaging_manager_api} after {max_retries}"))
      raise

def copy_ranger_kafka_jars():
  import params
  import glob

  kafka_libs_path = os.path.join(params.original_stack_root + "/current/kafka-broker/libs")
  smm_libs_path = os.path.join(params.stack_root + "/current/streams-messaging-manager/libs")
  ranger_kafka_plugin_shim_jar = os.path.join(kafka_libs_path, "ranger-kafka-plugin-shim-*.jar")
  ranger_plugin_classloader_jar = os.path.join(kafka_libs_path, "ranger-plugin-classloader-*.jar")

  if len(glob.glob(ranger_kafka_plugin_shim_jar)) > 0:
    cp_cmd = as_sudo(["cp", glob.glob(ranger_kafka_plugin_shim_jar)[0], smm_libs_path])
    Execute(cp_cmd, logoutput=True)

  if len(glob.glob(ranger_plugin_classloader_jar)) > 0:
    cp_cmd = as_sudo(["cp", glob.glob(ranger_plugin_classloader_jar)[0], smm_libs_path])
    Execute(cp_cmd, logoutput=True)

  ranger_kafka_plugin_impl_path = os.path.join(kafka_libs_path, "ranger-kafka-plugin-impl")
  if sudo.path_isdir(ranger_kafka_plugin_impl_path):
    cp_cmd = as_sudo(["cp","-r",ranger_kafka_plugin_impl_path, smm_libs_path])
    Execute(cp_cmd,logoutput = True)
