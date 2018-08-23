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

package com.hortonworks.smm.kafka.webservice.resources.metrics;

import com.hortonworks.smm.kafka.common.utils.AdminClientUtil;
import com.hortonworks.smm.kafka.services.management.BrokerManagementService;
import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.TopicInfo;
import com.hortonworks.smm.kafka.services.metric.cache.MetricsCache;
import com.hortonworks.smm.kafka.services.metric.dtos.BrokerDetails;
import com.hortonworks.smm.kafka.services.metric.dtos.ClusterWithBrokerMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.ClusterWithTopicMetrics;
import com.hortonworks.smm.kafka.webservice.extension.KafkaAdminRestServiceTest;
import com.hortonworks.smm.kafka.webservice.wrapper.LocalKafkaAdminServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;

import javax.ws.rs.core.Response;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;

@KafkaAdminRestServiceTest(numBrokerNodes = 3, kafkaConfig = {"log.retention.hours=1", "log.retention.minutes=59", "log.retention.ms=3480000", "offsets.topic.replication.factor=1", "offsets.topic.num.partitions=50"})
public class AggregatedMetricsResourceTest {

    private static final int TIMEOUT_MS = 60 * 1000;
    private static String AGGREGATED_METRICS_RESOURCE_PATH = "/api/v1/admin/metrics/aggregated/%s";

    @TestTemplate
    public void testClusterWithTopicMetrics(AdminClient adminClient, LocalKafkaAdminServer localKafkaAdminServer, TestInfo testInfo) throws Exception {

        int noConsumerOffsetsTopicPartitions = 50;
        int noOfInternalTopics = 1;
        int noTopics = 2;
        int numPartitions = 3;
        Short numReplicas = 3;
        String topicPrefix = testInfo.getTestMethod().get().getName();

        for (int i = 0; i < noTopics; i++) {
            AdminClientUtil.createTopic(adminClient, new NewTopic(topicPrefix + i, numPartitions, numReplicas), TIMEOUT_MS);
        }

        TopicManagementService topicManagementService = localKafkaAdminServer.getInjector().getInstance(TopicManagementService.class);
        topicManagementService.syncCache();

        MetricsCache metricsCache = localKafkaAdminServer.getInjector().getInstance(MetricsCache.class);
        metricsCache.refresh();

        Response response = localKafkaAdminServer.getWebTarget(buildURL("topics")).
                queryParam("duration", "LAST_THIRTY_MINUTES").
                queryParam("state", "all").request().get();
        ClusterWithTopicMetrics clusterWithTopicMetrics = response.readEntity(ClusterWithTopicMetrics.class);

        // Here the expected number of insync replicas would be sum(topic_num_partitions[i]*topic_num_replicas[i]) for all the topics
        // which comes out to be (3*3+3*3) = 18
        Assertions.assertEquals(18L, clusterWithTopicMetrics.inSyncReplicas().longValue() - noConsumerOffsetsTopicPartitions, "Mismatch in number of insync replicas");
        Assertions.assertEquals(0L, clusterWithTopicMetrics.outOfSyncReplicas().longValue(), "Mismatch in number of out of sync replicas");
        Assertions.assertEquals(0L, clusterWithTopicMetrics.underReplicatedPartitions().longValue(), "Mismatch in number of under replicated partitions");
        Assertions.assertEquals(2 , clusterWithTopicMetrics.aggrTopicMetricsCollection().size() - noOfInternalTopics, "Mismatch in number of topic metrics");

        for (int i = 0; i < 2; i++) {
            AdminClientUtil.deleteTopic(adminClient, topicPrefix + i, TIMEOUT_MS);
        }
    }

    @TestTemplate
    public void testClusterWithBrokerMetrics(LocalKafkaAdminServer localKafkaAdminServer) {
        Response response = localKafkaAdminServer.getWebTarget(buildURL("brokers")).
                queryParam("duration", "LAST_THIRTY_MINUTES").request().get();
        ClusterWithBrokerMetrics clusterWithBrokerMetrics = response.readEntity(ClusterWithBrokerMetrics.class);
        Assertions.assertEquals(3, clusterWithBrokerMetrics.aggrBrokerMetricsCollection().size());
    }

    @TestTemplate
    public void testBrokerDetails(AdminClient adminClient, LocalKafkaAdminServer localKafkaAdminServer, TestInfo testInfo) throws Exception {

        int noTopics = 2;
        int numPartitions = 3;
        Short numReplicas = 3;
        String topicPrefix = testInfo.getTestMethod().get().getName();

        for (int i = 0; i < noTopics; i++) {
            AdminClientUtil.createTopic(adminClient, new NewTopic(topicPrefix + i, numPartitions, numReplicas), TIMEOUT_MS);
        }

        // Sync Management service cache
        BrokerManagementService brokerManagementService = localKafkaAdminServer.getInjector().getInstance(BrokerManagementService.class);
        brokerManagementService.syncCache();
        TopicManagementService topicManagementService = localKafkaAdminServer.getInjector().getInstance(TopicManagementService.class);
        topicManagementService.syncCache();
        MetricsCache metricsCache = localKafkaAdminServer.getInjector().getInstance(MetricsCache.class);
        metricsCache.refresh();

        Response response = localKafkaAdminServer.getWebTarget(buildURL("brokers/0")).
                queryParam("duration", "LAST_THIRTY_MINUTES").request().get();
        BrokerDetails brokerDetails = response.readEntity(BrokerDetails.class);

        TopicInfo consumerOffsetsTopicInfo = topicManagementService.topicInfo(GROUP_METADATA_TOPIC_NAME);
        long noOfConsumerOffsetsReplicasOnbroker = consumerOffsetsTopicInfo.partitions().stream().
            filter(topicPartitionInfo -> topicPartitionInfo.leader().equals(brokerDetails.brokerNode())).count();

        // There are 2 (topics) * 3 (partitions) * 3 (replication_factor) = 18 replicas in the cluster, so in ideal scenario we should have
        // 6 replica per broker (18/3) and all of them should be in sync and number of leader partition per broker
        // should be (3 (partitions in topic0) + 3 (partitions in topic1)) / 3 (total number of brokers) = 2
        Assertions.assertEquals(6, brokerDetails.totalNumReplicas().intValue() - noOfConsumerOffsetsReplicasOnbroker, "Mismatch in total number of replicas");
        Assertions.assertEquals(6, brokerDetails.totalNumInSyncReplicas().intValue() - noOfConsumerOffsetsReplicasOnbroker, "Mismatch in total number of in sync replicas");
        Assertions.assertEquals(2, brokerDetails.topicLeaderPartitionInfo().size() - noOfConsumerOffsetsReplicasOnbroker, "Mismatch in total number of leader partitions in the broker");
        Assertions.assertEquals(3480000, brokerDetails.logRetentionPeriodValue().intValue(), "Mismatch in log retention period value");
        Assertions.assertEquals(TimeUnit.MILLISECONDS.name(), brokerDetails.logRetentionPeriodTimeUnit(), "Mismatch in log retention period time unit");

        for (int i = 0; i < 2; i++) {
            AdminClientUtil.deleteTopic(adminClient, topicPrefix + i, TIMEOUT_MS);
        }
    }

    private String buildURL(String path) {
        return String.format(AGGREGATED_METRICS_RESOURCE_PATH, path);
    }
}
