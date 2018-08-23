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

from resource_management import *
from resource_management.libraries.script.script import Script
from resource_management.core.logger import Logger
from resource_management.libraries.functions.format import format
from resource_management.libraries.functions.get_user_call_output import get_user_call_output
from resource_management.core.exceptions import ExecutionFailed
from resource_management.libraries.functions.show_logs import show_logs
from resource_management.core import shell

import urllib2, time, json

CURL_CONNECTION_TIMEOUT = '5'


class ServiceCheck(Script):
  def service_check(self, env):
    import params
    env.set_params(params)
    if params.streams_messaging_manager_ssl_enabled:
      streams_messaging_manager_api = format("https://{params.hostname}:{params.streams_messaging_manager_ssl_port}/api/v1/admin/brokers")
    else:
      streams_messaging_manager_api = format("http://{params.hostname}:{params.streams_messaging_manager_port}/api/v1/admin/brokers")
    Logger.info(streams_messaging_manager_api)
    max_retries = 8
    success = False

    if params.kerberos_security_enabled:
      kinit_cmd = format("{kinit_path_local} -kt {smoke_user_keytab} {smokeuser_principal};")
      return_code, out = shell.checked_call(kinit_cmd,
                                            path='/usr/sbin:/sbin:/usr/local/bin:/bin:/usr/bin',
                                            user=params.smokeuser,
                                            )

    for num in range(0, max_retries):
      try:
        Logger.info(format("Making http requests to {streams_messaging_manager_api}"))
        response = urllib2.urlopen(streams_messaging_manager_api)
        api_response = response.read()
        response_code = response.getcode()
        Logger.info(format("Streams Messaging Manager response http status {response}"))
        if response.getcode() != 200:
          Logger.error(format("Failed to fetch response for {streams_messaging_manager_api}"))
          show_logs(params.streams_messaging_manager_log_dir, params.streams_messaging_manager_user)
          raise
        else:
          success = True
          Logger.info(format("Successfully made a API request to Streams Messaging Manager. {api_response}"))
          break
      except (urllib2.URLError, ExecutionFailed) as e:
        Logger.error(
          format("Failed to make API request to Streams Messaging Manager server at {streams_messaging_manager_api},retrying.. {num} out of {max_retries}"))
        time.sleep(num * 10)  # exponential back-off
        continue

    if success != True:
      Logger.error(format("Failed to make API request to  Streams Messaging Manager server at {streams_messaging_manager_api} after {max_retries}"))
      raise


if __name__ == "__main__":
  ServiceCheck().execute()


