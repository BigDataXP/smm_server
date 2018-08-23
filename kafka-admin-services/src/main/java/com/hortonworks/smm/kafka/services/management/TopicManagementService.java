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
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.management.dtos.TopicInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicSummary;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;

import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 *
 */
@Singleton
public class TopicManagementService implements Service {

    private Cache cache;

    @Inject
    public TopicManagementService(BrokerManagementService brokerManagementService,
                                  AdminClient adminClient,
                                  KafkaAdminClientConfig kafkaAdminClientConfig,
                                  KafkaManagementConfig kafkaManagementConfig) {
        brokerManagementService.syncCache();
        this.cache = new Cache(adminClient,
                               kafkaAdminClientConfig.getRequestTimeoutMs(),
                               kafkaManagementConfig.cacheRefreshIntervalMs());
    }

    public Set<String> allTopicNames() {
        return cache.allTopicNames();
    }

    public Collection<TopicInfo> allTopicInfos() {
        return topicInfos(allTopicNames());
    }

    /**
     * @param topicNames names of topics
     * @param throwError if it is true throw any error received while fetching respective topic details
     *                   else it ignores any errors received while fetching and return the successful topic results.
     * @return Collection of {@link TopicInfo} for the given topic names
     */
    public Collection<TopicInfo> topicInfos(Collection<String> topicNames, boolean throwError) {
        Collection<TopicInfo> result = new ArrayList<>();
        Map<String, TopicInfo> allTopicInfo = cache.allTopicToTopicInfos();

        for (String topicName : topicNames) {
            if (allTopicInfo.containsKey(topicName)) {
                result.add(allTopicInfo.get(topicName));
            } else {
                if (throwError) {
                    throw new RuntimeException(String.format("Failed to get topicInfo for topic '%s' ", topicName));
                }
            }
        }

        return result;
    }

    /**
     * @param topicNames names of topics.
     * @return Collection of {@link TopicInfo} for the given topic names and throws any error received while fetching
     * topic details.
     */
    public Collection<TopicInfo> topicInfos(Collection<String> topicNames) {
        return topicInfos(topicNames, true);
    }

    public static Collection<TopicInfo> toTopicInfos(Map<String, TopicDescription> topicDescriptions) {
        List<TopicInfo> result = new ArrayList<>();
        for (TopicDescription topicDescription : topicDescriptions.values()) {
            result.add(TopicInfo.from(topicDescription));
        }

        return Collections.unmodifiableList(result);
    }

    public TopicInfo topicInfo(String topicName) {
        Collection<TopicInfo> topicInfos = topicInfos(Collections.singleton(topicName));
        if (topicInfos == null || topicInfos.isEmpty()) {
            throw new NotFoundException("Topic with name `" + topicName + "` does not exist");
        }

        return topicInfos.iterator().next();
    }

    public Map<String, Collection<TopicPartitionInfo>> topicPartitionInfos(Collection<String> topicPartitionStrings,
                                                                           SMMAuthorizer authorizer,
                                                                           SecurityContext securityContext) {
        if (topicPartitionStrings == null || topicPartitionStrings.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, Set<Integer>> topicPartitions = new HashMap<>();
        Function<String, Set<Integer>> setGenerator = value -> new HashSet<>();
        for (String str : topicPartitionStrings) {
            int index = str.lastIndexOf("-");
            String topicName = str.substring(0, index);

            if (!SecurityUtil.authorizeTopicDescribe(authorizer, securityContext, topicName))
                continue;

            topicPartitions.computeIfAbsent(topicName, setGenerator)
                           .add(Integer.parseInt(str.substring(index + 1)));
        }

        Map<String, Collection<TopicPartitionInfo>> topicPartitionInfos = new HashMap<>(topicPartitions.size());
        Map<String, Collection<TopicPartitionInfo>> allTopicPartitionInfos = cache.allTopicToTopicPartitionInfos();

        Function<String, Collection<TopicPartitionInfo>> listGenerator = value -> new ArrayList<>();
        for (String topicName : topicPartitions.keySet()) {
            Set<Integer> partitions = topicPartitions.get(topicName);
            for (TopicPartitionInfo topicPartitionInfo : allTopicPartitionInfos.get(topicName)) {
                if (partitions.contains(topicPartitionInfo.partition())) {
                    topicPartitionInfos.computeIfAbsent(topicName, listGenerator)
                                       .add(topicPartitionInfo);
                }
            }
        }

        return topicPartitionInfos;
    }

    public Collection<TopicPartitionInfo> topicPartitions(String topicName) {
        Map<String, Collection<TopicPartitionInfo>> allTopicPartitionInfos = cache.allTopicToTopicPartitionInfos();

        if (!allTopicPartitionInfos.containsKey(topicName)) {
            throw new NotFoundException("Topic `" + topicName + "` does not exist");
        }

        return Collections.unmodifiableCollection(allTopicPartitionInfos.get(topicName));
    }

    public TopicSummary getTopicSummary(TopicInfo topicInfo) {
        short numOfReplication = Integer.valueOf(topicInfo.partitions().get(0).replicas().size()).shortValue();
        int numOfPartition = topicInfo.partitions().size();
        Integer numOfleadersAsPreferredReplica = 0;
        Integer numOfUnderReplicatedPartitions = 0;

        Set<BrokerNode> brokersForTopic = new HashSet<>();
        for (TopicPartitionInfo topicPartitionInfo : topicInfo.partitions()) {
            brokersForTopic.addAll(topicPartitionInfo.replicas());
            if (topicPartitionInfo.leader().equals(topicPartitionInfo.replicas().get(0)))
                numOfleadersAsPreferredReplica++;
            if (topicPartitionInfo.replicas().size() > topicPartitionInfo.isr().size())
                numOfUnderReplicatedPartitions++;
        }
        int numOfBrokersForTopic = brokersForTopic.size();
        double preferredReplicasPercent = (numOfleadersAsPreferredReplica.doubleValue() / numOfPartition) * 100;
        double underReplicatedPercent = (numOfUnderReplicatedPartitions.doubleValue() / numOfPartition) * 100;

        return new TopicSummary(numOfReplication, numOfPartition,
                numOfBrokersForTopic, preferredReplicasPercent, underReplicatedPercent, topicInfo.isInternal());
    }

    @VisibleForTesting
    public void syncCache() throws Exception {
        cache.syncCache();
    }

    @Override
    public void close() throws Exception {
        cache.close();
    }

    private static class Cache extends AbstractManagementServiceCache {

        private static final ListTopicsOptions LIST_TOPICS_OPTIONS = new ListTopicsOptions().listInternal(true);
        private AtomicReference<Set<String>> allTopicNamesReference;
        private AtomicReference<Map<String, TopicInfo>> allTopicToTopicInfosReference;
        private AtomicReference<Map<String, Collection<TopicPartitionInfo>>> allTopicToTopicPartitionInfosReference;


        public Cache(AdminClient adminClient, Integer adminClientRequestTimeoutMs, Long cacheRefreshIntervalMs) {
            super(adminClient, "topic-management-service", adminClientRequestTimeoutMs, cacheRefreshIntervalMs);

            allTopicNamesReference = new AtomicReference<>(fetchAllTopicNames());
            allTopicToTopicInfosReference = new AtomicReference<>(fetchAllTopicToTopicInfos());
            allTopicToTopicPartitionInfosReference = new AtomicReference<>(fetchAllTopicToTopicPartitionInfos());
        }

        @Override
        public void syncCache() {
            allTopicNamesReference.set(fetchAllTopicNames());
            allTopicToTopicInfosReference.set(fetchAllTopicToTopicInfos());
            allTopicToTopicPartitionInfosReference.set(fetchAllTopicToTopicPartitionInfos());
        }

        public Set<String> allTopicNames() {
            return allTopicNamesReference.get();
        }

        public Map<String, TopicInfo> allTopicToTopicInfos() {
            return allTopicToTopicInfosReference.get();
        }

        public Map<String, Collection<TopicPartitionInfo>> allTopicToTopicPartitionInfos() {
            return allTopicToTopicPartitionInfosReference.get();
        }

        private Set<String> fetchAllTopicNames() {
            return resultFromFuture(adminClient.listTopics(LIST_TOPICS_OPTIONS).names());
        }

        private Map<String, TopicInfo> fetchAllTopicToTopicInfos() {
            Map<String, TopicInfo> allTopicInfos = new HashMap<>();

            for (TopicInfo topicInfo : toTopicInfos(resultFromFuture(
                    adminClient.describeTopics(allTopicNamesReference.get()).all()))) {
                allTopicInfos.put(topicInfo.name(), topicInfo);
            }

            return allTopicInfos;
        }

        private Map<String, Collection<TopicPartitionInfo>> fetchAllTopicToTopicPartitionInfos() {
            Map<String, Collection<TopicPartitionInfo>> topicToTopicPartitionInfos = new HashMap<>();
            Map<String, TopicDescription> topicDescriptions =
                    resultFromFuture(adminClient.describeTopics(allTopicNames()).all());

            for (TopicDescription topicDescription : topicDescriptions.values()) {
                List<TopicPartitionInfo> topicPartitionInfos = new ArrayList<>();
                for (org.apache.kafka.common.TopicPartitionInfo topicPartitionInfo : topicDescription.partitions()) {
                    topicPartitionInfos.add(TopicPartitionInfo.from(topicPartitionInfo));
                }
                topicToTopicPartitionInfos.put(topicDescription.name(), topicPartitionInfos);
            }

            return topicToTopicPartitionInfos;
        }
    }
}
