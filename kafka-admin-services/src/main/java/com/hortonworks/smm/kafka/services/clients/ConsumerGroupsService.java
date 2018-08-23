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
package com.hortonworks.smm.kafka.services.clients;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.smm.kafka.common.config.KafkaAdminClientConfig;
import com.hortonworks.smm.kafka.common.config.KafkaConsumerConfig;
import com.hortonworks.smm.kafka.common.config.KafkaMetricsConfig;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerGroupInfo;
import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

@Singleton
public class ConsumerGroupsService {

    public static final String CG_FETCHER_GROUP_ID_PREFIX = "__smm_consumer_group_fetcher";
    public static final int DEFAULT_COMMIT_OFFSETS_REFRESH_INTERVAL_MS = 5 * 60 * 1000;
    public static final String COMMIT_OFFSETS_REFRESH_INTERVAL_MS = "smm.commit.offsets.refresh.interval.ms";

    private ConsumerGroupFetcherTask consumerGroupFetcherTask;

    @Inject
    public ConsumerGroupsService(KafkaAdminClientConfig adminClientConfig,
                                 KafkaConsumerConfig consumerConfig,
                                 KafkaMetricsConfig metricsConfig,
                                 TopicManagementService topicManagementService) {
        Objects.requireNonNull(adminClientConfig, "adminClientConfig must not be null");
        Objects.requireNonNull(consumerConfig, "consumerConfig must not be null");
        Objects.requireNonNull(metricsConfig, "metricsConfig must not be null");
        Objects.requireNonNull(topicManagementService, "topicManagementService must not be null");

        createKafkaConsumer(consumerConfig, adminClientConfig, metricsConfig, topicManagementService);
    }

    private void createKafkaConsumer(KafkaConsumerConfig consumerConfig,
                                     KafkaAdminClientConfig adminClientConfig,
                                     KafkaMetricsConfig metricsConfig,
                                     TopicManagementService topicManagementService) {
        ExecutorService executorService = Executors.newFixedThreadPool(1,
                                                                       new ThreadFactoryBuilder()
                                                                               .setDaemon(true)
                                                                               .setNameFormat(
                                                                                       "consumer-group-fetcher-%d")
                                                                               .build());

        Supplier<Set<org.apache.kafka.common.TopicPartition>> topicPartitionSupplier =
                () -> {
                    Set<org.apache.kafka.common.TopicPartition> allTopicPartitions = new HashSet<>();
                    Set<String> allTopics = topicManagementService.allTopicNames();
                    topicManagementService.topicInfos(allTopics, false)
                                          .forEach(topicInfo -> {
                                              String name = topicInfo.name();
                                              topicInfo.partitions()
                                                       .stream()
                                                       .filter(TopicPartitionInfo::leaderExists)
                                                       .forEach(partitionInfo ->
                                                                        allTopicPartitions
                                                                                .add(new org.apache.kafka.common.TopicPartition(
                                                                                        name,
                                                                                        partitionInfo
                                                                                                .partition())));
                                          });

                    return allTopicPartitions;
                };

        consumerGroupFetcherTask = new ConsumerGroupFetcherTask(consumerConfig, adminClientConfig, metricsConfig.getInactiveGroupTimeoutMs(), topicPartitionSupplier);
        executorService.submit(consumerGroupFetcherTask);
    }

    void waitTillRefresh() throws Exception {
        consumerGroupFetcherTask.waitTillRefresh();
    }

    /**
     * Get all group Ids
     *
     * @return
     */
    public Collection<String> getGroupNames() {
        return consumerGroupInfos().keySet();
    }

    /**
     * Get All Consumer Groups
     *
     * @return
     */
    public Collection<ConsumerGroupInfo> getAllConsumerGroups() {
        return consumerGroupInfos().values();
    }

    /**
     * Get All Consumer Groups for given group names
     *
     * @param groupNames
     *
     * @return
     */
    public List<ConsumerGroupInfo> getConsumerGroups(List<String> groupNames) {
        List<ConsumerGroupInfo> result = new ArrayList<>(groupNames.size());
        for (String groupName : groupNames) {
            ConsumerGroupInfo consumerGroupInfo = consumerGroupInfos().get(groupName);
            if (consumerGroupInfo == null) {
                consumerGroupInfo = new ConsumerGroupInfo(groupName, InvalidGroupStatus.GROUP_DEAD.name());
            }
            result.add(consumerGroupInfo);
        }

        return result;
    }

    private Map<String, ConsumerGroupInfo> consumerGroupInfos() {
        return consumerGroupFetcherTask.consumerGroupInfos();
    }


    /**
     * Get Consumer group
     *
     * @param group
     *
     * @return
     */
    public ConsumerGroupInfo getConsumerGroup(String group) {
        // this will always return a list with an item or it throws an Exception
        return getConsumerGroups(Collections.singletonList(group)).iterator().next();
    }

    /**
     * Close the clients
     */
    public void close() throws Exception {
        Utils.closeQuietly(consumerGroupFetcherTask, "ConsumerGroupFetcherTask");
    }

}
