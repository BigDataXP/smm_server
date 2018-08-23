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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.smm.kafka.common.config.KafkaAdminClientConfig;
import com.hortonworks.smm.kafka.common.config.KafkaManagementConfig;
import com.hortonworks.smm.kafka.common.errors.NotFoundException;
import com.hortonworks.smm.kafka.services.Service;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerLogDirInfos;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.management.dtos.KafkaClusterInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class BrokerManagementService implements Service {

    private Cache cache;

    @Inject
    public BrokerManagementService(AdminClient adminClient,
                                   KafkaAdminClientConfig kafkaAdminClientConfig,
                                   KafkaManagementConfig kafkaManagementConfig) {
        this.cache = new Cache(adminClient,
                               kafkaAdminClientConfig.getRequestTimeoutMs(),
                               kafkaManagementConfig.cacheRefreshIntervalMs());
    }

    public KafkaClusterInfo describeCluster() {
        return cache.kafkaClusterInfo();
    }

    public Collection<BrokerNode> allBrokers() {
        Collection<BrokerNode> allBrokerNodes = describeCluster().brokerNodes();

        if (allBrokerNodes == null || allBrokerNodes.isEmpty()) {
            return Collections.emptyList();
        } else {
            return allBrokerNodes;
        }
    }

    /**
     * Returns broker nodes in the cluster with the given {@code brokerIds} if they exist in the target cluster.
     *
     * @param brokerIds broker ids
     * @return
     */
    public Collection<BrokerNode> brokers(Collection<Integer> brokerIds) {
        List<BrokerNode> filteredBrokerNodes = new ArrayList<>();

        for (BrokerNode brokerNode : allBrokers()) {
            if (brokerIds.contains(brokerNode.id())) {
                filteredBrokerNodes.add(brokerNode);
            }
        }

        return Collections.unmodifiableList(filteredBrokerNodes);
    }

    public BrokerLogDirInfos allBrokersDescribeLogDirs() {
        return BrokerLogDirInfos.from(cache.allBrokerIdToLogDirInfo());
    }

    public BrokerLogDirInfos describeLogDirs(Collection<Integer> brokerIds) {
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> allLogDirs = cache.allBrokerIdToLogDirInfo();
        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> logDirs = new HashMap<>();

        for (Integer brokerId : brokerIds) {
            if (allLogDirs.containsKey(brokerId)) {
                logDirs.put(brokerId, allLogDirs.get(brokerId));
            } else {
                throw new NotFoundException(String.format("Failed to get log dir info for the broker '%s'", brokerId));
            }
        }

        return BrokerLogDirInfos.from(logDirs);
    }

    @VisibleForTesting
    public void syncCache() {
        cache.syncCache();
    }

    @Override
    public void close() throws Exception {
        cache.close();
    }

    private static class Cache extends AbstractManagementServiceCache {

        private AtomicReference<KafkaClusterInfo> kafkaClusterInfoReference;
        private AtomicReference<Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>>> allBrokerIdToLogDirInfoReference;

        public Cache(AdminClient adminClient,
                     Integer adminClientRequestTimeoutMs,
                     Long cacheRefreshIntervalMs) {
            super(adminClient, "broker-management-service", adminClientRequestTimeoutMs,  cacheRefreshIntervalMs);

            this.kafkaClusterInfoReference = new AtomicReference<>(fetchDescribeCluster());
            this.allBrokerIdToLogDirInfoReference = new AtomicReference<>(fetchAllBrokersIdToDescribeLogDirs());

        }

        @Override
        public void syncCache() {
            kafkaClusterInfoReference.set(fetchDescribeCluster());
            allBrokerIdToLogDirInfoReference.set(fetchAllBrokersIdToDescribeLogDirs());
        }

        public KafkaClusterInfo kafkaClusterInfo() {
            return this.kafkaClusterInfoReference.get();
        }

        public Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> allBrokerIdToLogDirInfo() {
            return this.allBrokerIdToLogDirInfoReference.get();
        }

        private KafkaClusterInfo fetchDescribeCluster() {
            DescribeClusterResult describeClusterResult = adminClient.describeCluster();
            String clusterId = resultFromFuture(describeClusterResult.clusterId());
            Node controllerNode = resultFromFuture(describeClusterResult.controller());
            Collection<Node> clusterNodes = resultFromFuture(describeClusterResult.nodes());
            BrokerNode.refreshPool(clusterNodes);

            return KafkaClusterInfo.from(clusterId, controllerNode, clusterNodes);
        }

        private Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> fetchAllBrokersIdToDescribeLogDirs() {
            List<Integer> brokerIds = new ArrayList<>();
            for (BrokerNode brokerNode : kafkaClusterInfoReference.get().brokerNodes()) {
                brokerIds.add(brokerNode.id());
            }
            return resultFromFuture(adminClient.describeLogDirs(brokerIds).all());
        }
    }
}
