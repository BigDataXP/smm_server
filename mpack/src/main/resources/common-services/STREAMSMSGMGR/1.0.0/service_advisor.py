#!/usr/bin/env ambari-python-wrap
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

import os
import fnmatch
import imp
import socket
import sys
import traceback

# Local imports
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STACKS_DIR = os.path.join(SCRIPT_DIR, '../../../../../stacks/')
PARENT_FILE = os.path.join(STACKS_DIR, 'service_advisor.py')

try:
  with open(PARENT_FILE, 'rb') as fp:
    service_advisor = imp.load_module('service_advisor', fp, PARENT_FILE, ('.py', 'rb', imp.PY_SOURCE))
except Exception as e:
  traceback.print_exc()
  print "Failed to load parent"

class STREAMSMSGMGR100ServiceAdvisor(service_advisor.ServiceAdvisor):

  def __init__(self, *args, **kwargs):
    self.as_super = super(STREAMSMSGMGR100ServiceAdvisor, self)
    self.as_super.__init__(*args, **kwargs)
    self.initialize_logger("STREAMSMSGMGR100ServiceAdvisor")

  def getServiceConfigurationRecommendations(self, configurations, clusterSummary, services, hosts):
    self.logger.info("Class: %s, Method: %s. Get Service Configuration Recommendations." % (self.__class__.__name__, inspect.stack()[0][3]))
    servicesList = [service["StackServices"]["service_name"] for service in services["services"]]

    streams_messaging_manger_common = self.getServicesSiteProperties(services, "streams-messaging-manager-common")
    putStreamsMessagingManagerCommonProperty = self.putProperty(configurations, "streams-messaging-manager-common")

    kafka_brokers_site = self.getServicesSiteProperties(services, "kafka-broker")
    if kafka_brokers_site is not None:
      # Getting kafka broker host:port info.
      kafka_brokers_list = self.getHostNamesWithComponent("KAFKA", services)
      if len(kafka_brokers_list) > 0:
        kafka_bootstrap_servers = ','.join(
          [each_host + ":" + kafka_brokers_site['port'] for each_host in kafka_brokers_list])
        putStreamsMessagingManagerCommonProperty("kafka.bootstrap.servers", kafka_bootstrap_servers)

    registry_common_site = self.getServicesSiteProperties(services, "registry-common")
    if registry_common_site is not None:
      # Getting Schema Registry host:port info
      schema_registry_host = self.getHostNamesWithComponent("REGISTRY", services)[0]
      if len(schema_registry_host) > 0:
        smm_schema_registry_url = "http" + "://" + schema_registry_host + ":" + registry_common_site['port'] + "/api/v1"
        putStreamsMessagingManagerCommonProperty("schema.registry.url", smm_schema_registry_url)
    else:
      putStreamsMessagingManagerCommonProperty("schema.registry.url", "")

    ams_site = self.getServicesSiteProperties(services, "ams-site")
    if ams_site is not None:
      # Getting AMS host and port info.
      ams_timeline_metrics_host = self.getHostNamesWithComponent("AMBARI_METRICS", services)[0]
      ams_timeline_metrics_port = ams_site['timeline.metrics.service.webapp.address'].split(":")[1]
      putStreamsMessagingManagerCommonProperty("ams.timeline.metrics.hosts", ams_timeline_metrics_host)
      putStreamsMessagingManagerCommonProperty("ams.timeline.metrics.port", ams_timeline_metrics_port)

    #Setting up KNOX SSO for Streams Messaging Manager.
    self.getServiceConfigurationRecommendationsForSSO(configurations, clusterSummary, services, hosts)

  def getServiceConfigurationRecommendationsForSSO(self, configurations, clusterSummary, services, hosts):
    """
    Any SSO-related configuration recommendations for the service should be defined in this function.
    """
    ambari_configuration = self.get_ambari_configuration(services)
    ambari_sso_details = ambari_configuration.get_ambari_sso_details() if ambari_configuration else None

    if ambari_sso_details and ambari_sso_details.is_managing_services():
      putStreamsMessagingManagerSSOProperty = self.putProperty(configurations, "streams-messaging-manager-sso-config")

      # If SSO should be enabled for this service
      if ambari_sso_details.should_enable_sso('STREMSMSGMGR'):
        putStreamsMessagingManagerSSOProperty("streams_messaging_manager.sso.enabled", "true")
        putStreamsMessagingManagerSSOProperty("streams_messaging_manager.authentication.provider.url", ambari_sso_details.get_sso_provider_url())
        putStreamsMessagingManagerSSOProperty("streams_messaging_manager.public.key.pem", ambari_sso_details.get_sso_provider_certificate(False, True))
      else:
        putStreamsMessagingManagerSSOProperty("streams_messaging_manager.sso.enabled", "false")

  def getServiceComponentLayoutValidations(self, services, hosts):
    """
    Returns an array of Validation objects about issues with the hostnames to which components are assigned.
    This should detect validation issues which are different than those the stack_advisor.py detects.
    The default validations are in stack_advisor.py getComponentLayoutValidations function.
    """
    return []

  def getServiceConfigurationsValidationItems(self, configurations, recommendedDefaults, services, hosts):
    """
    Any configuration validations for the service should be defined in this function.
    This should be similar to any of the validateXXXXConfigurations functions in the stack_advisor.py
    such as validateHDFSConfigurations.
    """

    validator = StremsMsgMgrValidator()
    # Calls the methods of the validator using arguments,
    # method(siteProperties, siteRecommendations, configurations, services, hosts)
    return validator.validateListOfConfigUsingMethod(configurations, recommendedDefaults, services, hosts, validator.validators)

  def getHostNamesWithComponent(self, serviceName, services):
    """
    Returns the list of hostnames on which service component is installed
    """
    number_services = len(services['services'])
    for each_service in range(0, number_services):
      if services['services'][each_service]['components'][0]['StackServiceComponents']['service_name'] == serviceName:
        componentHostnames = services['services'][each_service]['components'][0]['StackServiceComponents']['hostnames']
        return componentHostnames
    return []

  '''
  def colocateService(self, hostsComponentsMap, serviceComponents):
    # colocate Streams Messaging Manager with Kafka Broker, if no hosts have been allocated for Streams Messaging Manager
    streamsMsgMgrSegment = [component for component in serviceComponents if component["StackServiceComponents"]["component_name"] == "STREAMSMSGMGR"][0]
    if not self.isComponentHostsPopulated(streamsMsgMgrSegment):
      for hostName in hostsComponentsMap.keys():
        hostComponents = hostsComponentsMap[hostName]
        if {"name": "KAFKA_BROKER"} in hostComponents and {"name": "STREAMSMSGMGR"} not in hostComponents:
          hostsComponentsMap[hostName].append({"name": "STREAMSMSGMGR"})
          break
        if {"name": "KAFKA_BROKER"} not in hostComponents and {"name": "STREAMSMSGMGR"} in hostComponents:
          hostComponents.remove({"name": "STREAMSMSGMGR"})
  '''

  def getSiteProperties(self, configurations, siteName):
    siteConfig = configurations.get(siteName)
    if siteConfig is None:
      return None
    return siteConfig.get("properties")

  def getServicesSiteProperties(self, services, siteName):
    configurations = services.get("configurations")
    if not configurations:
      return None
    siteConfig = configurations.get(siteName)
    if siteConfig is None:
      return None
    return siteConfig.get("properties")

  def putProperty(self, config, configType, services=None):
    userConfigs = {}
    changedConfigs = []
    # if services parameter, prefer values, set by user
    if services:
      if 'configurations' in services.keys():
        userConfigs = services['configurations']
      if 'changed-configurations' in services.keys():
        changedConfigs = services["changed-configurations"]

    if configType not in config:
      config[configType] = {}
    if "properties" not in config[configType]:
      config[configType]["properties"] = {}
    def appendProperty(key, value):
      # If property exists in changedConfigs, do not override, use user defined property
      if self.__isPropertyInChangedConfigs(configType, key, changedConfigs):
        config[configType]["properties"][key] = userConfigs[configType]['properties'][key]
      else:
        config[configType]["properties"][key] = str(value)
    return appendProperty

  def __isPropertyInChangedConfigs(self, configType, propertyName, changedConfigs):
    for changedConfig in changedConfigs:
      if changedConfig['type'] == configType and changedConfig['name'] == propertyName:
        return True
    return False

class StremsMsgMgrValidator(service_advisor.ServiceAdvisor):
  """
    Streams Messaging Manager Validator checks the correctness of properties whenever the service is first added or the user attempts to
    change configs via the UI.
    """

  def __init__(self, *args, **kwargs):
    self.as_super = super(StremsMsgMgrValidator, self)
    self.as_super.__init__(*args, **kwargs)

    self.validators = [("streams-messaging-manager-common", self.validateSTREAMLSMESSAGINGMANAGERConfigurationsCommon),
                       ("streams-messaging-manager-sso-config", self.validateSTREAMLSMESSAGINGMANAGERConfigurationsSSO)]


  def validateSTREAMLSMESSAGINGMANAGERConfigurationsCommon(self, properties, recommendedDefaults, configurations, services, hosts):

    validationItems = []
    streams_messaging_manager_database_name = services['configurations']['streams-messaging-manager-common']['properties']['database_name']
    streams_messaging_manager_storage_type = str(services['configurations']['streams-messaging-manager-common']['properties']['streams_messaging_manager.storage.type']).lower()
    streams_messaging_manager_storage_connector_connectURI = services['configurations']['streams-messaging-manager-common']['properties']['streams_messaging_manager.storage.connector.connectURI']
    url_error_message = ""

    import re
    if streams_messaging_manager_storage_connector_connectURI:
      if 'oracle' not in streams_messaging_manager_storage_type:
        pattern = '(.*?):(.*?)://(.*?):(.*?)/(.*)'
        dbc_connector_uri = re.match(pattern, streams_messaging_manager_storage_connector_connectURI)
        if dbc_connector_uri is not None:
          dbc_connector_type, db_storage_type, smm_db_hostname, smm_db_portnumber, smm_db_name = re.match(
            pattern, streams_messaging_manager_storage_connector_connectURI).groups()
          if (not dbc_connector_type or not dbc_connector_type or not smm_db_hostname or not smm_db_portnumber or not smm_db_name):
            url_error_message += "Please enter Streams Messaging Manager storage connector url in following format jdbc:" + streams_messaging_manager_storage_type \
                                 + "://smm_db_hostname:port_number/" + streams_messaging_manager_database_name
            validationItems.append({"config-name": 'streamline.storage.connector.connectURI', "item": self.getErrorItem(url_error_message)})
        else:
          url_error_message += "Please enter Streams Messaging Manager storage connector url in following format jdbc:" \
                               + streams_messaging_manager_storage_type + "://streamline_db_hostname:port_number/" + streams_messaging_manager_database_name
          validationItems.append({"config-name": 'streams_messaging_manager.storage.connector.connectURI', "item": self.getErrorItem(url_error_message)})
      else:
        pattern = '(.*?):(.*?):(.*?):@(.*?):(.*?)/(.*)'
        dbc_connector_uri = re.match(pattern, streams_messaging_manager_storage_connector_connectURI)
        if dbc_connector_uri is not None:
          dbc_connector_type, db_storage_type, dbc_connector_kind, smm_db_hostname, smm_db_portnumber, smm_db_name = re.match(
            pattern, streams_messaging_manager_storage_connector_connectURI).groups()
          if (not dbc_connector_type or not db_storage_type or not dbc_connector_kind or not smm_db_hostname or not smm_db_portnumber or not smm_db_name):
            url_error_message += "Please enter Streams Messaging Manager storage connector url in following format jdbc:" + streams_messaging_manager_storage_type \
                                 + ":thin:@smm_db_hostname:port_number/" + streams_messaging_manager_database_name
            validationItems.append({"config-name": 'streams_messaging_manager.storage.connector.connectURI', "item": self.getErrorItem(url_error_message)})
        else:
          url_error_message += "Please enter Streams Messaging Manager storage connector url in following format jdbc:" + streams_messaging_manager_storage_type \
                               + ":thin:@smm_db_hostname:port_number/" + streams_messaging_manager_database_name
          validationItems.append({"config-name": 'streams_messaging_manager.storage.connector.connectURI', "item": self.getErrorItem(url_error_message)})

    validationProblems = self.toConfigurationValidationProblems(validationItems, "streams-messaging-manager-common")
    return validationProblems


  def validateSTREAMLSMESSAGINGMANAGERConfigurationsSSO(self, properties, recommendedDefaults, configurations, services, hosts):

    # SSO validations
    validationItems = []
    streams_messaging_manager_sso_enabled = services['configurations']['streams-messaging-manager-sso-config']['properties']['streams_messaging_manager.sso.enabled']
    streams_messaging_manager_authenication_url = services['configurations']['streams-messaging-manager-sso-config']['properties']['streams_messaging_manager.authentication.provider.url']
    streams_messaging_manager_public_key = services['configurations']['streams-messaging-manager-sso-config']['properties']['streams_messaging_manager.public.key.pem']

    if streams_messaging_manager_sso_enabled.lower() == 'true':
      if ((len(streams_messaging_manager_authenication_url) < 1) or (len(streams_messaging_manager_public_key) < 1)):
        SSO_message = ""
        if len(streams_messaging_manager_authenication_url) < 1:
          SSO_message += "If Streams Messaging Manager SSO is enabled, Please enter authenticaion url."
          validationItems.append({"config-name": 'streams_messaging_manager.authentication.provider.url', "item": self.getErrorItem(SSO_message)})
        if len(streams_messaging_manager_public_key) < 1:
          SSO_message += "If Streams Messaging Manager SSO is enabled, Please enter public key pem."
          validationItems.append({"config-name": 'streams_messaging_manager.public.key.pem', "item": self.getErrorItem(SSO_message)})

    validationProblems = self.toConfigurationValidationProblems(validationItems, "streams-messaging-sso-config")
    return validationProblems


