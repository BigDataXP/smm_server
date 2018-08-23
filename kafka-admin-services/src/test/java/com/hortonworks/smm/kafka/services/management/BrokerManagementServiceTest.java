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

import com.hortonworks.smm.kafka.services.extension.KafkaAdminServiceTest;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerLogDirInfos;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.management.dtos.KafkaClusterInfo;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.requests.DescribeLogDirsResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@KafkaAdminServiceTest(numBrokerNodes = 3)
@DisplayName("Broker management service tests")
public class BrokerManagementServiceTest {

    @TestTemplate
    @DisplayName("Test describe log dirs for a broker")
    public void describeLogDirs(BrokerManagementService brokerManagementService, AdminClient adminClient) throws Exception {
        List<Integer> brokerIds = adminClient.describeCluster().nodes().get().stream().map(Node::id).collect(Collectors.toList());

        Map<Integer, Map<String, DescribeLogDirsResponse.LogDirInfo>> expectedLogDirInfos = adminClient.describeLogDirs(brokerIds).all().get();
        BrokerLogDirInfos actualLogDirInfos = brokerManagementService.describeLogDirs(brokerIds);
        assertEquals(BrokerLogDirInfos.from(expectedLogDirInfos), actualLogDirInfos);
    }

    @TestTemplate
    @DisplayName("Test describe cluster")
    public void describeCluster(BrokerManagementService brokerManagementService, AdminClient adminClient) throws Exception {
        DescribeClusterResult clusterResult = adminClient.describeCluster();
        KafkaClusterInfo actualClusterInfo = brokerManagementService.describeCluster();

        assertEquals(BrokerNode.from(clusterResult.controller().get()), actualClusterInfo.controller());
        assertEquals(clusterResult.clusterId().get(), actualClusterInfo.clusterId());
        assertEquals(clusterResult.nodes().get().stream().map(BrokerNode::from).collect(Collectors.toList()), actualClusterInfo.brokerNodes());
    }

    @TestTemplate
    @DisplayName("Test get all brokers")
    public void allBrokers(BrokerManagementService brokerManagementService, AdminClient adminClient) throws Exception {
        Collection<BrokerNode> expectedNodes = adminClient.describeCluster().nodes().get()
                .stream().map(BrokerNode::from).collect(Collectors.toList());
        Collection<BrokerNode> actualNodes = brokerManagementService.allBrokers();
        assertEquals(expectedNodes, actualNodes);
    }

    @TestTemplate
    @DisplayName("Test get broker by broker id")
    public void brokers(BrokerManagementService brokerManagementService) throws Exception {
        assertEquals(Collections.<BrokerNode>emptyList(), brokerManagementService.brokers(Collections.emptyList()));

        Collection<Integer> brokerIds = Arrays.asList(0, 2);
        Collection<BrokerNode> brokers = brokerManagementService.brokers(brokerIds);
        assertEquals(brokerIds, brokers.stream().map(BrokerNode::id).collect(Collectors.toList()));
    }

    @TestTemplate
    @DisplayName("Test all brokers describe log dirs")
    public void allBrokersDescribeLogDirs(BrokerManagementService brokerManagementService) throws Exception {
        List<Integer> brokerIds = brokerManagementService.allBrokers().stream().map(BrokerNode::id).collect(Collectors.toList());
        assertEquals(brokerManagementService.describeLogDirs(brokerIds), brokerManagementService.allBrokersDescribeLogDirs());
    }
}