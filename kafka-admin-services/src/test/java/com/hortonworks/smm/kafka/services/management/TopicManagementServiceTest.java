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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.hortonworks.smm.kafka.common.config.KafkaConsumerConfig;
import com.hortonworks.smm.kafka.common.utils.AdminClientUtil;
import com.hortonworks.smm.kafka.services.extension.KafkaAdminServiceTest;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.management.dtos.TopicInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfos;
import com.hortonworks.smm.kafka.services.management.dtos.TopicSummary;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;

/**
 *
 */

@KafkaAdminServiceTest(numBrokerNodes = 3)
@DisplayName("Topic management service tests")
public class TopicManagementServiceTest {

    public static final Logger LOG = LoggerFactory.getLogger(TopicManagementServiceTest.class);

    private static final int TIME_OUT_MS = 60 * 1000;

    @TestTemplate
    @DisplayName("Test fetch topic and topic info")
    public void testTopicApis(AdminClient adminClient, TopicManagementService topicManagementService) throws Exception {
        List<String> topicNames = IntStream.range(1, 8).boxed().map(x -> "topic-" + x).collect(Collectors.toList());

        final List<NewTopic> topicsToCreate = new ArrayList<>();
        for (String topic : topicNames) {
            topicsToCreate.add(new NewTopic(topic, 3, (short) 2));
        }

        AdminClientUtil.createTopics(adminClient, topicsToCreate, TIME_OUT_MS);
        topicManagementService.syncCache();

        Collection<TopicInfo> expectedTopicInfos =
                TopicManagementService.toTopicInfos(adminClient.describeTopics(topicNames).all().get());

        Set<String> allTopicNames = topicManagementService.allTopicNames();
        allTopicNames.remove(GROUP_METADATA_TOPIC_NAME);
        Assertions.assertEquals(new HashSet<>(topicNames), allTopicNames);

        Collection<TopicInfo> topicInfos = topicManagementService.topicInfos(topicNames);

        Assertions.assertEquals(Sets.newHashSet(expectedTopicInfos), Sets.newHashSet(topicInfos));

        AdminClientUtil.deleteTopics(adminClient, topicNames, TIME_OUT_MS);
    }

    @TestTemplate
    @DisplayName("Test fetch broker nodes")
    public void testBrokerApis(AdminClient adminClient, BrokerManagementService brokerManagementService) throws Exception {
        brokerManagementService.syncCache();
        Collection<BrokerNode> brokerNodes = brokerManagementService.allBrokers();

        final Set<BrokerNode> nodes = new HashSet<>();
        for (Node x : adminClient.describeCluster().nodes().get()) {
            nodes.add(BrokerNode.from(x));
        }

        Assertions.assertEquals(nodes, new HashSet<>(brokerNodes));
    }

    @TestTemplate
    @DisplayName("Test ser/des TopicPartitionInfos pojo")
    public void testTopicPartitionInfoPojos() throws Exception {

        Map<String, Collection<TopicPartitionInfo>> entries = new HashMap<>();

        for (int i = 1; i < 3; i++) {
            entries.computeIfAbsent("topic", y -> new ArrayList<>())
                   .add(TopicPartitionInfo.from(
                           new org.apache.kafka.common.TopicPartitionInfo(i,
                                                                          new Node(1, "foo", 6111),
                                                                          Collections.emptyList(),
                                                                          Collections.emptyList())));
        }

        TopicPartitionInfos topicPartitionInfos = new TopicPartitionInfos(entries);
        LOG.info("topicPartitionInfos [{}] ", topicPartitionInfos);

        ObjectMapper objectMapper = new ObjectMapper();
        String resultStr = objectMapper.writeValueAsString(topicPartitionInfos);
        LOG.info("resultStr [{}] ", resultStr);

        TopicPartitionInfos resultObj = objectMapper.readValue(resultStr, TopicPartitionInfos.class);

        LOG.info("resultObj [{}]", resultObj);

        Assertions.assertEquals(topicPartitionInfos, resultObj);
    }

    @TestTemplate
    @DisplayName("Test brokers with null or NO_NODE and other values")
    public void testTopicLeaderWithNullOrNoNode() throws Exception {
        int num = Math.abs(new Random().nextInt(10000));
        Node node = new Node(num, "host-" + num, num + 1, "rack-" + num);
        BrokerNode.refreshPool(Collections.singleton(node));

        BrokerNode brokerNode = BrokerNode.from(node);
        Assertions.assertEquals(node, brokerNode.toNode());

        Assertions.assertNotNull(BrokerNode.NO_NODE);

        Node nullNode = null;
        Assertions.assertEquals(BrokerNode.NO_NODE, BrokerNode.from(nullNode));

        Node noNode = Node.noNode();
        Assertions.assertEquals(BrokerNode.NO_NODE, BrokerNode.from(noNode));
    }

    @TestTemplate
    @DisplayName("Test topic summary")
    public void testTopicSummary(AdminClient adminClient, KafkaConsumerConfig consumerConfig, TopicManagementService topicManagementService) throws Exception {

        String topicName = "test";
        int numPartitions = 3;
        short numReplicas = Integer.valueOf(2).shortValue();

        AdminClientUtil.createTopic(adminClient, new NewTopic(topicName, numPartitions, numReplicas), TIME_OUT_MS);

        // create consumer offsets topic
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, consumerConfig.getConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG,  "test-group");
        IntegrationTestUtils.waitUntilMinValuesRecordsReceived(consumerProps, topicName, 0);

        topicManagementService.syncCache();

        TopicInfo topicInfo = topicManagementService.topicInfo(topicName);
        TopicSummary actualTopicSummary = topicManagementService.getTopicSummary(topicInfo);
        TopicSummary expectedTopicSummary = new TopicSummary(numReplicas, numPartitions, numPartitions, 100, 0, false);
        Assert.assertEquals(expectedTopicSummary, actualTopicSummary);

        TopicInfo consumerOffsetsTopicInfo = topicManagementService.topicInfo(GROUP_METADATA_TOPIC_NAME);
        TopicSummary consumerOffsetTopicSummary = topicManagementService.getTopicSummary(consumerOffsetsTopicInfo);
        Assertions.assertTrue(consumerOffsetTopicSummary.isInternal());

        AdminClientUtil.deleteTopic(adminClient, topicName, TIME_OUT_MS);

    }

}
