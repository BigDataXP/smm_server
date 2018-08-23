/*
 *   HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
 *
 *   (c) 2016-2018 Hortonworks, Inc. All rights reserved.
 *
 *   This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
 *   Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
 *   to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
 *   properly licensed third party, you do not have any rights to this code.
 *
 *   If this code is provided to you under the terms of the AGPLv3:
 *   (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
 *   (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
 *     LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
 *   (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
 *     FROM OR RELATED TO THE CODE; AND
 *   (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
 *     DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
 *     DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
 *     OR LOSS OR CORRUPTION OF DATA.
 */
package com.hortonworks.smm.kafka.services.management;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.smm.kafka.common.config.KafkaAdminClientConfig;
import com.hortonworks.smm.kafka.common.config.KafkaManagementConfig;
import com.hortonworks.smm.kafka.services.Service;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.management.dtos.KafkaResourceConfig;
import com.hortonworks.smm.kafka.services.management.dtos.KafkaResourceConfigEntry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.common.config.ConfigResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 *
 */

@Singleton
public class ResourceConfigsService implements Service {

    private Cache cache;

    @Inject
    public ResourceConfigsService(AdminClient adminClient,
                                  KafkaAdminClientConfig kafkaAdminClientConfig,
                                  KafkaManagementConfig kafkaManagementConfig,
                                  BrokerManagementService brokerManagementService,
                                  TopicManagementService topicManagementService) {
        this.cache = new Cache(adminClient,
                               kafkaAdminClientConfig.getRequestTimeoutMs(),
                               kafkaManagementConfig.cacheRefreshIntervalMs(),
                               brokerManagementService,
                               topicManagementService);
    }

    public Collection<KafkaResourceConfig> allTopicConfigs() {
        return cache.allTopicNameToConfigMap().values();
    }

    public Collection<KafkaResourceConfig> topicConfigs(Collection<String> topicNames) {
        if (topicNames == null || topicNames.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, KafkaResourceConfig> kafkaResourceConfigMap = cache.allTopicNameToConfigMap();
        Set<KafkaResourceConfig> topicConfigs = new HashSet<>();
        for (String topicName : topicNames) {
            if (kafkaResourceConfigMap.containsKey(topicName)) {
                topicConfigs.add(kafkaResourceConfigMap.get(topicName));
            }
        }

        return topicConfigs;
    }

    public Collection<KafkaResourceConfig> allBrokerConfigs() {
        return cache.allBrokerIdToConfigMap().values();
    }

    public Collection<KafkaResourceConfig> brokerConfigs(Collection<Integer> brokerIds) {
        if (brokerIds == null || brokerIds.isEmpty()) {
            return Collections.emptyList();
        }

        Map<Integer, KafkaResourceConfig> kafkaResourceConfigMap = cache.allBrokerIdToConfigMap();
        Set<KafkaResourceConfig> brokerConfigs = new HashSet<>();
        for (Integer brokerId : brokerIds) {
            if (kafkaResourceConfigMap.containsKey(brokerId)) {
                brokerConfigs.add(kafkaResourceConfigMap.get(brokerId));
            }
        }

        return brokerConfigs;
    }

    @Override
    public void close() throws Exception {
        cache.close();
    }

    private static class Cache extends AbstractManagementServiceCache {
        private BrokerManagementService brokerManagementService;
        private TopicManagementService topicManagementService;

        private AtomicReference<Map<Integer, KafkaResourceConfig>> brokerIdToConfigReference;
        private AtomicReference<Map<String, KafkaResourceConfig>> topicNameToConfigReference;

        public Cache(AdminClient adminClient,
                     Integer adminClientRequestTimeoutMs,
                     Long cacheRefreshIntervalMs,
                     BrokerManagementService brokerManagementService,
                     TopicManagementService topicManagementService) {
            super(adminClient, "resource-config-management-service", adminClientRequestTimeoutMs, cacheRefreshIntervalMs);

            this.brokerManagementService = brokerManagementService;
            this.topicManagementService = topicManagementService;

            brokerIdToConfigReference = new AtomicReference<>(fetchAllBrokerIdToConfigMap());
            topicNameToConfigReference = new AtomicReference<>(fetchAllTopicNameToConfigMap());
        }

        @Override
        public void syncCache() {
            brokerIdToConfigReference.set(fetchAllBrokerIdToConfigMap());
            topicNameToConfigReference.set(fetchAllTopicNameToConfigMap());
        }

        public Map<Integer, KafkaResourceConfig> allBrokerIdToConfigMap() {
            return brokerIdToConfigReference.get();
        }

        public Map<String, KafkaResourceConfig> allTopicNameToConfigMap() {
            return topicNameToConfigReference.get();
        }

        private Map<Integer, KafkaResourceConfig> fetchAllBrokerIdToConfigMap() {
            Map<Integer, KafkaResourceConfig> allBrokerIdToConfigMap = new HashMap<>();

            Collection<BrokerNode> allBrokerNodes = brokerManagementService.allBrokers();
            if (allBrokerNodes == null || allBrokerNodes.isEmpty())
                return allBrokerIdToConfigMap;

            Set<ConfigResource> brokerResources = new HashSet<>();
            for (BrokerNode brokerNode : allBrokerNodes) {
                brokerResources.add(new ConfigResource(ConfigResource.Type.BROKER, Integer.valueOf(brokerNode.id()).toString()));
            }

            for (KafkaResourceConfig kafkaResourceConfig : toKafkaResourceConfigs(getResourceConfig(brokerResources))) {
                allBrokerIdToConfigMap.put(Integer.valueOf(kafkaResourceConfig.name()), kafkaResourceConfig);
            }

            return allBrokerIdToConfigMap;
        }

        private Map<String, KafkaResourceConfig> fetchAllTopicNameToConfigMap() {
            Map<String, KafkaResourceConfig> allTopicNameToConfigMap = new HashMap<>();

            Collection<String> allTopicNames = topicManagementService.allTopicNames();
            if (allTopicNames == null || allTopicNames.isEmpty())
                return allTopicNameToConfigMap;

            Set<ConfigResource> topicResources = new HashSet<>();
            for (String topicName : allTopicNames) {
                topicResources.add(new ConfigResource(ConfigResource.Type.TOPIC, topicName));
            }

            for (KafkaResourceConfig kafkaResourceConfig : toKafkaResourceConfigs(getResourceConfig(topicResources))) {
                allTopicNameToConfigMap.put(kafkaResourceConfig.name(), kafkaResourceConfig);
            }

            return allTopicNameToConfigMap;
        }

        private Map<ConfigResource, Config> getResourceConfig(Set<ConfigResource> configResources) {
            return resultFromFuture(adminClient.describeConfigs(configResources).all());
        }

        private Collection<KafkaResourceConfig> toKafkaResourceConfigs(Map<ConfigResource, Config> configs) {
            List<KafkaResourceConfig> result = new ArrayList<>();
            for (Map.Entry<ConfigResource, Config> entry : configs.entrySet()) {
                result.add(new KafkaResourceConfig(entry.getKey().type(),
                        entry.getKey().name(),
                        entry.getValue()
                                .entries()
                                .stream()
                                .map(x -> KafkaResourceConfigEntry.from(x))
                                .collect(Collectors.toList())));
            }

            return Collections.unmodifiableList(result);
        }
    }
}