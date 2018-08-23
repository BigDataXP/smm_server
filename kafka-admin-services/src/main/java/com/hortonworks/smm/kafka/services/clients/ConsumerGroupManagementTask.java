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

import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerGroupInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerPartitionInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerPartitionOffsetInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.PartitionAssignment;
import com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

class ConsumerGroupManagementTask implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupManagementTask.class);

    private final ConsumerGroupsService consumerGroupsService;
    private final MetricsService metricsService;
    private volatile ConsumerGroups consumerGroups = ConsumerGroups.EMPTY;

    ConsumerGroupManagementTask(ConsumerGroupsService consumerGroupsService,
                                MetricsService metricsService) {
        this.consumerGroupsService = consumerGroupsService;
        this.metricsService = metricsService;
    }

    @Override
    public synchronized void run() {
        try {
            long startTimeMs = System.currentTimeMillis();
            Collection<String> consumerGroupNames = consumerGroupsService.getGroupNames();
            List<ConsumerGroupInfo> consumerGroupInfos =
                    consumerGroupsService.getConsumerGroups(new ArrayList<>(consumerGroupNames));
            Map<String, ConsumerGroupInfo> cgIdToInfos =
                    consumerGroupInfos.stream().collect(Collectors.toMap(ConsumerGroupInfo::id, c -> c));
            Map<String, List<ConsumerPartitionInfo>> clientIdToPartitions =
                    getConsumerPartitions(consumerGroupInfos);

            LOG.debug("All consumerGroupNames : {}", consumerGroupNames);
            LOG.debug("consumerGroupInfos : {}", consumerGroupInfos);
            LOG.debug("clientToPartitions : {}", clientIdToPartitions);
            LOG.info("Updated the Consumer Group Info. Time taken to fetch all consumer consumerGroupInfos with "
                     + "size [{}] : {} ms", consumerGroupInfos.size(), (System.currentTimeMillis() - startTimeMs));

            // sync with shared values
            consumerGroups = new ConsumerGroups(cgIdToInfos, clientIdToPartitions);

            emitMetrics(clientIdToPartitions, consumerGroups);
        } catch (Exception e) {
            LOG.error("Error while fetching consumer group information.", e);
        }
    }

    ConsumerGroups consumerGroups() {
        return consumerGroups;
    }

    private void emitMetrics(Map<String, List<ConsumerPartitionInfo>> clientIdToPartitions,
                             ConsumerGroups consumerGroups) {
        long startTimeMs = System.currentTimeMillis();
        Map<MetricDescriptor, Long> metrics = new HashMap<>();

        //prepare consumer group partition metrics
        clientIdToPartitions.forEach((clientId, consumerPartitions) -> {
            for (ConsumerPartitionInfo consumerPartition : consumerPartitions) {
                for (Map.Entry<String, Map<Integer, ConsumerPartitionOffsetInfo>> topicEntry : consumerPartition
                        .topicPartitionOffsets().entrySet()) {
                    for (Map.Entry<Integer, ConsumerPartitionOffsetInfo> partitionOffsetInfoEntry : topicEntry
                            .getValue().entrySet()) {

                        Map<String, String> tags = getGroupPartitionTags(clientId, consumerPartition.getGroupId(),
                                                                         topicEntry.getKey(),
                                                                         partitionOffsetInfoEntry.getKey());

                        MetricDescriptor partitionLag = metricsService.getSupplier().partitionLag(tags);
                        MetricDescriptor partitionCommittedOffsets =
                                metricsService.getSupplier().partitionCommittedOffset(tags);
                        metrics.put(partitionLag,
                                    getOrDefault(partitionOffsetInfoEntry.getValue().lag(),
                                                 -1L));
                        metrics.put(partitionCommittedOffsets,
                                    getOrDefault(partitionOffsetInfoEntry.getValue().offset(),
                                                 -1L));
                    }
                }
            }
        });

        //prepare group metrics
        for (ConsumerGroupInfo groupInfo : consumerGroups.all()) {

            long lag = 0L;
            long committedOffset = 0L;

            for (Map<Integer, PartitionAssignment> partitionAssignmentMap : groupInfo.topicPartitionAssignments()
                                                                                     .values()) {
                for (PartitionAssignment partitionAssignment : partitionAssignmentMap.values()) {
                    lag = lag + partitionAssignment.lag();
                    committedOffset = committedOffset + partitionAssignment.offset();
                }
            }

            Map<String, String> tags = new LinkedHashMap<>();
            tags.put(AbstractMetricDescriptorSupplier.CONSUMER_GROUP, groupInfo.id());

            MetricDescriptor totalLag = metricsService.getSupplier().groupLag(tags);
            MetricDescriptor totalCommittedOffsets = metricsService.getSupplier().groupCommittedOffset(tags);
            metrics.put(totalLag, lag);
            metrics.put(totalCommittedOffsets, committedOffset);
        }

        LOG.debug("consumer group metrics : {}", metrics);
        metricsService.emitMetrics(metrics);
        LOG.info("Time taken to push consumer groups metrics : {} ms", (System.currentTimeMillis() - startTimeMs));
    }

    private Map<String, String> getGroupPartitionTags(String clientId, String groupId, String topic,
                                                      Integer partition) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(AbstractMetricDescriptorSupplier.CLIENT_ID, clientId);
        tags.put(AbstractMetricDescriptorSupplier.CONSUMER_GROUP, groupId);
        tags.put(AbstractMetricDescriptorSupplier.TOPIC, topic);
        tags.put(AbstractMetricDescriptorSupplier.PARTITION_NUMBER, Integer.toString(partition));
        return tags;
    }

    private Long getOrDefault(Long lag, Long defaultValue) {
        return lag != null ? lag : defaultValue;
    }


    /**
     * This gives mapping between consumer client.id to consumer group partition assignment
     *
     * @return
     */
    private Map<String, List<ConsumerPartitionInfo>> getConsumerPartitions(List<ConsumerGroupInfo> groups) {
        Map<String, List<ConsumerPartitionInfo.Builder>> consumerPartitionInfoBuilders = new HashMap<>();

        for (ConsumerGroupInfo cg : groups) {

            if (!validGroup(cg)) {
                continue;
            }

            Map<String, ConsumerPartitionInfo.Builder> consumerPartitionBuilders = new HashMap<>();
            for (Map.Entry<String, Map<Integer, PartitionAssignment>> topicEntry : cg
                    .topicPartitionAssignments().entrySet()) {
                for (Map.Entry<Integer, PartitionAssignment> partitionAssignmentEntry : topicEntry.getValue()
                                                                                                  .entrySet()) {
                    PartitionAssignment partitionAssignment = partitionAssignmentEntry.getValue();
                    String clientId = partitionAssignment.clientId();
                    ConsumerPartitionInfo.Builder consumerPartition =
                            consumerPartitionBuilders.computeIfAbsent(clientId,
                                                                  id -> new ConsumerPartitionInfo.Builder(cg.id()));
                    Integer partition = partitionAssignmentEntry.getKey();
                    consumerPartition.addTopicPartitionLag(topicEntry.getKey(),
                                                           partition,
                                                           partitionAssignment.offset(),
                                                           partitionAssignment.lag(),
                                                           partitionAssignment.commitTimestamp());
                }
            }

            consumerPartitionBuilders.forEach((clientId, consumerPartitionInfo) -> {
                consumerPartitionInfoBuilders.computeIfAbsent(clientId, id -> new LinkedList<>())
                                             .add(consumerPartitionInfo);
            });
        }

        final HashMap<String, List<ConsumerPartitionInfo>> result = new HashMap<>();
        consumerPartitionInfoBuilders
                .forEach((id, builderList) -> {
                             for (ConsumerPartitionInfo.Builder builder : builderList) {
                                 result.computeIfAbsent(id, x -> new ArrayList<>())
                                       .add(builder.build());
                             }
                         }
                );
        return result;
    }

    private boolean validGroup(ConsumerGroupInfo cg) {
        for (InvalidGroupStatus invalidSate : InvalidGroupStatus.values()) {
            if (invalidSate.name.equals(cg.state())) {
                return false;
            }
        }
        return true;
    }
}
