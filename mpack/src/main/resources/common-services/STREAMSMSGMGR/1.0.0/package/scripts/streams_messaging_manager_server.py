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

from resource_management import Script
from resource_management.core.logger import Logger
from resource_management.core.resources.system import Execute, File, Directory
from resource_management.libraries.functions import conf_select
from resource_management.libraries.functions import stack_select
from resource_management.libraries.functions import default
from resource_management.libraries.functions import Direction
from resource_management.libraries.functions.version import format_stack_version
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.check_process_status import check_process_status
from resource_management.libraries.functions import StackFeature
from resource_management.libraries.functions.stack_features import check_stack_feature
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.core.shell import as_sudo
from resource_management.core import sudo
#from ambari_commons.os_family_impl import OsFamilyFuncImpl, OsFamilyImpl

import os, time, shutil, glob
from streams_messaging_manager import ensure_base_directories
from streams_messaging_manager import streams_messaging_manager, wait_until_server_starts

#@OsFamilyImpl(os_family=OsFamilyImpl.DEFAULT)
class StreamsMessagingManagerServer(Script):

  def execute_bootstrap(self, params):
    Execute(params.bootstrap_storage_run_cmd + ' migrate',
            user=params.streams_messaging_manager_user)

  def configure(self, env, upgrade_type=None):
    import params
    env.set_params(params)
    streams_messaging_manager(env, upgrade_type=None)

  def install(self, env):
    import params
    env.set_params(params)
    self.install_packages(env)

  def start(self, env, upgrade_type=None):
    import params
    import status_params
    env.set_params(params)
    self.configure(env)
    self.execute_bootstrap(params)
    start_cmd = format('source {params.conf_dir}/streams-messaging-manager-env.sh ; {params.streams_messaging_manager_script} start')
    no_op_test = format('ls {status_params.streams_messaging_manager_pid_file} >/dev/null 2>&1 && ps -p `cat {status_params.streams_messaging_manager_pid_file}` >/dev/null 2>&1')
    try:
      Execute(start_cmd,
              user="root",
              not_if=no_op_test
              )
    except:
      show_logs(params.streams_messaging_manager_log_dir, params.streams_messaging_manager_user)
      raise

    try:
      wait_until_server_starts()
    except:
      show_logs(params.streams_messaging_manager_log_dir, params.streams_messaging_manager_user)
      raise

  def stop(self, env, upgrade_type=None):
    import params
    import status_params
    env.set_params(params)
    ensure_base_directories()

    daemon_cmd = format('source {params.conf_dir}/streams-messaging-manager-env.sh; {params.streams_messaging_manager_script} stop')
    try:
      Execute(daemon_cmd,
              user="root",
              )
    except:
      show_logs(params.streams_messaging_manager_log_dir, params.streams_messaging_manager_user)
      raise
    File(status_params.streams_messaging_manager_pid_file,
         action="delete"
         )

  def pre_upgrade_restart(self, env, upgrade_type=None):
      pass

  def status(self, env):
      import status_params
      env.set_params(status_params)
      check_process_status(status_params.streams_messaging_manager_pid_file)

  def get_log_folder(self):
      import params
      return params.streams_messaging_manager_log_dir

  def get_user(self):
      import params
      return params.streams_messaging_manager_user

  def get_pid_files(self):
      import status_params
      return [status_params.streams_messaging_manager_pid_file]

if __name__ == "__main__":
    StreamsMessagingManagerServer().execute()