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
package com.hortonworks.smm.kafka.services.metric;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.smm.kafka.common.config.KafkaMetricsConfig;
import com.hortonworks.smm.kafka.services.clients.ClientState;
import com.hortonworks.smm.kafka.services.clients.ConsumerGroupManagementService;
import com.hortonworks.smm.kafka.services.clients.ProducerMetrics;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerGroupInfo;
import com.hortonworks.smm.kafka.services.management.BrokerManagementService;
import com.hortonworks.smm.kafka.services.management.ResourceConfigsService;
import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.management.dtos.KafkaResourceConfig;
import com.hortonworks.smm.kafka.services.management.dtos.KafkaResourceConfigEntry;
import com.hortonworks.smm.kafka.services.management.dtos.TopicInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicPartitionInfo;
import com.hortonworks.smm.kafka.services.management.dtos.TopicSummary;
import com.hortonworks.smm.kafka.services.metric.dtos.AggrBrokerMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.AggrConsumerMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.AggrProducerMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.AggrTopicMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.BrokerDetails;
import com.hortonworks.smm.kafka.services.metric.dtos.BrokerMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.ClusterWithBrokerMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.ClusterWithTopicMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.TopicMetrics;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hortonworks.smm.kafka.services.metric.MetricUtils.extractMaxTimestampValue;
import static com.hortonworks.smm.kafka.services.metric.MetricUtils.extractMinMaxTimestampDeltaValue;

@Singleton
public class AggregatedMetricsService {

    private static final Logger LOG = LoggerFactory.getLogger(AggregatedMetricsService.class);

    private MetricsService metricsService;
    private long inactiveProducerTimeoutMs;
    private BrokerManagementService brokerManagementService;
    private TopicManagementService topicManagementService;
    private ResourceConfigsService resourceConfigsService;
    private ConsumerGroupManagementService consumerGroupManagementService;
    private SMMAuthorizer authorizer;

    @Inject
    public AggregatedMetricsService(MetricsService metricsService,
                                    BrokerManagementService brokerManagementService,
                                    ResourceConfigsService resourceConfigsService,
                                    TopicManagementService topicManagementService,
                                    ConsumerGroupManagementService consumerGroupManagementService,
                                    KafkaMetricsConfig kafkaMetricsConfig,
                                    SMMAuthorizer authorizer) {
        this.metricsService = metricsService;
        this.brokerManagementService = brokerManagementService;
        this.resourceConfigsService = resourceConfigsService;
        this.topicManagementService = topicManagementService;
        this.consumerGroupManagementService = consumerGroupManagementService;
        this.inactiveProducerTimeoutMs = kafkaMetricsConfig.getInactiveProducerTimeoutMs();
        this.authorizer = authorizer;
    }

    public ClusterWithBrokerMetrics getAggrBrokerMetricsAtClusterScope(TimeSpan timeSpan) {

        Long totalBytesIn = extractMinMaxDeltaValueFromMetricsMap(metricsService.getClusterBytesInCount(timeSpan));
        Long totalBytesOut = extractMinMaxDeltaValueFromMetricsMap(metricsService.getClusterBytesOutCount(timeSpan));
        Double producedPerSec = extractMaxValueFromMetricsMap(metricsService.getClusterProduceRequestRate(timeSpan));
        Double fetchedPerSec = extractMaxValueFromMetricsMap(metricsService.getClusterFetchRequestRate(timeSpan));
        Integer activeControllerCount =
                extractMaxValueFromMetricsMap(metricsService.getActiveControllerCount(timeSpan)).intValue();
        Double uncleanLeaderElectionRate =
                extractMaxValueFromMetricsMap(metricsService.getUncleanLeaderElectionCount(timeSpan));
        Double requestPoolUsage =
                1 - extractMaxValueFromMetricsMap(metricsService.getClusterRequestPoolIdleUsageRate(timeSpan));
        Collection<AggrBrokerMetrics> aggrBrokerMetricsCollection = getAllBrokerMetrics(timeSpan);

        return new ClusterWithBrokerMetrics(totalBytesIn, totalBytesOut, producedPerSec, fetchedPerSec,
                                            activeControllerCount, uncleanLeaderElectionRate, requestPoolUsage,
                                            aggrBrokerMetricsCollection);
    }

    public Collection<AggrBrokerMetrics> getAllBrokerMetrics(TimeSpan timeSpan) {
        Collection<BrokerNode> brokerNodes = brokerManagementService.allBrokers();
        List<AggrBrokerMetrics> result = new ArrayList<>(brokerNodes.size());
        for (BrokerNode node : brokerNodes) {
            BrokerMetrics brokerMetrics = BrokerMetrics.from(node, metricsService, timeSpan);
            result.add(AggrBrokerMetrics.from(node, brokerMetrics));
        }
        return result;
    }

    public ClusterWithTopicMetrics getAggrTopicsMetricsAtClusterScope(String state,
                                                                      TimeSpan timeSpan,
                                                                      SecurityContext securityContext) {

        Long totalBytesIn = extractMinMaxDeltaValueFromMetricsMap(metricsService.getClusterBytesInCount(timeSpan));
        Long totalBytesOut = extractMinMaxDeltaValueFromMetricsMap(metricsService.getClusterBytesOutCount(timeSpan));
        Double producedPerSec = extractMaxValueFromMetricsMap(metricsService.getClusterProduceRequestRate(timeSpan));
        Double fetchedPerSec = extractMaxValueFromMetricsMap(metricsService.getClusterFetchRequestRate(timeSpan));
        Collection<AggrTopicMetrics> aggrTopicMetricsCollection = getAllTopicMetrics(timeSpan, state, securityContext);
        Long inSyncReplicas = 0L;
        Long outOfSyncReplicas = 0L;
        Long underReplicatedPartitions = 0L;
        Long offlinePartitions =
                Math.round(extractMaxValueFromMetricsMap(metricsService.getTotalOfflinePartitionsCount(timeSpan)));

        for (AggrTopicMetrics aggrTopicMetrics : aggrTopicMetricsCollection) {
            for (Map.Entry<Integer, AggrTopicMetrics.WrappedTopicPartitionMetrics> partitionMetric :
                    aggrTopicMetrics.partitionMetrics().entrySet()) {
                TopicPartitionInfo topicPartitionInfo =
                        partitionMetric.getValue().aggrTopicPartitionMetrics().partitionInfo();
                inSyncReplicas += topicPartitionInfo.isr().size();
                outOfSyncReplicas += (topicPartitionInfo.replicas().size() - topicPartitionInfo.isr().size());
                underReplicatedPartitions +=
                        (topicPartitionInfo.replicas().size() > topicPartitionInfo.isr().size() ? 1 : 0);
            }
        }

        return new ClusterWithTopicMetrics(totalBytesIn, totalBytesOut, producedPerSec, fetchedPerSec,
                                           inSyncReplicas, outOfSyncReplicas, underReplicatedPartitions,
                                           offlinePartitions, aggrTopicMetricsCollection);
    }

    public Collection<AggrTopicMetrics> getAllTopicMetrics(TimeSpan timeSpan, String stateStr,
                                                           SecurityContext securityContext) {
        ClientState clientState = ClientState.from(stateStr);

        Collection<ProducerMetrics> producerMetrics = ProducerMetrics.from(metricsService, inactiveProducerTimeoutMs,
                                                                           clientState, timeSpan, authorizer,
                                                                           securityContext);

        Collection<TopicInfo> topicInfos = SecurityUtil.filterTopics(authorizer,
                                                                     securityContext,
                                                                     topicManagementService.allTopicInfos(),
                                                                     TopicInfo::name);

        Collection<ConsumerGroupInfo> consumerGroups = consumerGroupManagementService.consumerGroups(clientState);

        List<AggrTopicMetrics> result = new ArrayList<>(topicInfos.size());
        for (TopicInfo topicInfo : topicInfos) {
            String topicName = topicInfo.name();
            TopicMetrics topicMetrics = TopicMetrics.from(topicName, metricsService, timeSpan);
            TopicSummary topicSummary = topicManagementService.getTopicSummary(topicInfo);
            Long topicRetentionMs = getTopicRetentionMs(topicName);
            result.add(AggrTopicMetrics.from(topicInfo, topicMetrics, producerMetrics, topicRetentionMs, topicSummary,
                                             consumerGroups, authorizer, securityContext));
        }

        return result;
    }

    public AggrTopicMetrics getTopicMetrics(String topicName, String state, TimeSpan timeSpan,
                                            SecurityContext securityContext) {
        ClientState clientState = ClientState.from(state);
        TopicInfo topicInfo = topicManagementService.topicInfo(topicName);
        if (topicInfo == null)
            return null;

        Collection<ProducerMetrics> producerMetrics = ProducerMetrics.from(metricsService, inactiveProducerTimeoutMs,
                                                                           clientState, timeSpan, authorizer,
                                                                           securityContext);
        TopicMetrics topicMetrics = TopicMetrics.from(topicName, metricsService, timeSpan);
        TopicSummary topicSummary = topicManagementService.getTopicSummary(topicInfo);
        Collection<ConsumerGroupInfo> consumerGroups = consumerGroupManagementService.consumerGroups(clientState);
        Long topicRetentionMs = getTopicRetentionMs(topicName);
        return AggrTopicMetrics.from(topicInfo, topicMetrics, producerMetrics, topicRetentionMs, topicSummary,
                                     consumerGroups, authorizer, securityContext);
    }

    public Collection<AggrProducerMetrics> getAllProducerMetrics(String state,
                                                                 TimeSpan timeSpan,
                                                                 SecurityContext securityContext) {
        ClientState clientState = ClientState.from(state);
        Collection<ProducerMetrics> producerMetrics = ProducerMetrics.from(metricsService, inactiveProducerTimeoutMs,
                                                                           clientState, timeSpan, authorizer,
                                                                           securityContext);
        Map<String, TopicInfo> topicInfos = topicInfosFromProducerMetrics(producerMetrics);
        Collection<ConsumerGroupInfo> consumerGroups = consumerGroupManagementService.consumerGroups(clientState);

        List<AggrProducerMetrics> result = new ArrayList<>(producerMetrics.size());
        for (ProducerMetrics prodMetrics : producerMetrics) {
            result.add(AggrProducerMetrics.from(prodMetrics,
                                                metricsService,
                                                topicInfos,
                                                timeSpan,
                                                consumerGroups,
                                                authorizer, securityContext));
        }
        return result;
    }

    public AggrProducerMetrics getProducerMetrics(String producerClientId,
                                                  TimeSpan timeSpan,
                                                  SecurityContext securityContext) {
        ProducerMetrics producerMetrics =
                ProducerMetrics.from(producerClientId, metricsService, inactiveProducerTimeoutMs, timeSpan,
                                     authorizer, securityContext);

        return AggrProducerMetrics.from(producerMetrics,
                                        metricsService,
                                        topicInfosFromProducerMetrics(Collections.singletonList(producerMetrics)),
                                        timeSpan,
                                        consumerGroupManagementService.allConsumerGroups(), authorizer,
                                        securityContext);
    }

    public Collection<AggrConsumerMetrics> getAllConsumerMetrics(boolean requireTimelineMetrics,
                                                                 String state,
                                                                 TimeSpan timeSpan,
                                                                 SecurityContext securityContext) {
        ClientState clientState = ClientState.from(state);
        Collection<ConsumerGroupInfo> consumerGroups = consumerGroupManagementService.consumerGroups(clientState);

        if (consumerGroups.isEmpty()) {
            return Collections.emptySet();
        }

        final Map<String, TopicInfo> topicInfos = topicInfosFromConsumerGroups(consumerGroups, securityContext);
        Collection<ProducerMetrics> producerMetrics = ProducerMetrics
                .from(metricsService, inactiveProducerTimeoutMs, clientState, timeSpan, authorizer, securityContext);
        List<AggrConsumerMetrics> aggrConsumerMetrics = AggrConsumerMetrics.from(consumerGroups, metricsService,
                topicInfos, requireTimelineMetrics, timeSpan, producerMetrics, authorizer, securityContext);
        return aggrConsumerMetrics;
    }

    public AggrConsumerMetrics getConsumerMetrics(String groupId,
                                                  boolean requireTimelineMetrics,
                                                  String state,
                                                  TimeSpan timeSpan,
                                                  SecurityContext securityContext) {
        if (!SecurityUtil.authorizeGroupDescribe(authorizer, securityContext, groupId)) {
            throw new javax.ws.rs.NotFoundException(groupId + " group not found");
        }

        ClientState clientState = ClientState.from(state);
        ConsumerGroupInfo groupInfo = consumerGroupManagementService.consumerGroup(groupId);

        if (groupInfo == null) {
            throw new javax.ws.rs.NotFoundException(groupId + " group not found");
        }

        return AggrConsumerMetrics.from(groupInfo,
                                        metricsService,
                                        topicInfosFromConsumerGroups(Collections.singletonList(groupInfo),
                                                                     securityContext),
                                        requireTimelineMetrics,
                                        timeSpan,
                                        ProducerMetrics
                                                .from(metricsService, inactiveProducerTimeoutMs, clientState, timeSpan,
                                                      authorizer, securityContext),
                                        authorizer, securityContext);
    }


    public BrokerDetails brokerDetails(Integer brokerId, TimeSpan timeSpan, SecurityContext securityContext) {
        Collection<BrokerNode> brokerNodes = brokerManagementService.brokers(Collections.singleton(brokerId));
        if (brokerNodes == null || brokerNodes.isEmpty())
            throw new NotFoundException(String.format("Invalid broker with id '%s'", brokerId));
        BrokerNode brokerNode = brokerNodes.iterator().next();

        Long logRetentionPeriodValue = -1L;
        String logRetentionPeriodTimeUnit = "INVALID";
        Collection<KafkaResourceConfig> kafkaResourceConfigs = resourceConfigsService.brokerConfigs(Collections.singleton(brokerId));
        if (kafkaResourceConfigs != null && !kafkaResourceConfigs.isEmpty()) {
            for (KafkaResourceConfigEntry kafkaResourceConfigEntry : kafkaResourceConfigs.iterator().next().resourceConfigs()) {
                String value = kafkaResourceConfigEntry.value();
                if (!Strings.isNullOrEmpty(value)) {
                    if (kafkaResourceConfigEntry.name().equals("log.retention.hours") &&
                            logRetentionPeriodTimeUnit.equals("INVALID")) {
                        logRetentionPeriodValue = Long.valueOf(value);
                        logRetentionPeriodTimeUnit = TimeUnit.HOURS.name();
                    } else if (kafkaResourceConfigEntry.name().equals("log.retention.minutes") &&
                            !logRetentionPeriodTimeUnit.equals(TimeUnit.MILLISECONDS.name())) {
                        logRetentionPeriodValue = Long.valueOf(value);
                        logRetentionPeriodTimeUnit = TimeUnit.MINUTES.name();
                    } else if (kafkaResourceConfigEntry.name().equals("log.retention.ms")) {
                        logRetentionPeriodValue = Long.valueOf(value);
                        logRetentionPeriodTimeUnit = TimeUnit.MILLISECONDS.name();
                    }
                }
            }
        }

        Map<MetricDescriptor, Map<Long, Double>> diskPercentage = metricsService.getDiskPercentMetrics(timeSpan, brokerNode);
        Map<MetricDescriptor, Map<Long, Double>> loadAvg = metricsService.getLoadFiveMetrics(timeSpan, brokerNode);
        Map<MetricDescriptor, Map<Long, Double>> memFreePercentage = metricsService.getMemFreePercentMetrics(timeSpan, brokerNode);
        Map<MetricDescriptor, Map<Long, Double>> cpuIdlePercentage = metricsService.getCpuIdleMetrics(timeSpan, brokerNode);

        Map<Long, Double> cpuUsagePercentageMetric = new HashMap<>();
        Map<Long, Double> diskUsagePercentageMetric = extractValue(diskPercentage);
        Map<Long, Double> cpuLoadMetric = extractValue(loadAvg);
        Map<Long, Double> memoryUsagePercentageMetrics = new HashMap<>();

        for (Map.Entry<Long, Double> cpuIdlePercentageMetric : extractValue(cpuIdlePercentage).entrySet()) {
            cpuUsagePercentageMetric.put(cpuIdlePercentageMetric.getKey(), 100 - cpuIdlePercentageMetric.getValue());
        }

        for (Map.Entry<Long, Double> memoryFreePercentageMetric : extractValue(memFreePercentage).entrySet()) {
            memoryUsagePercentageMetrics.put(memoryFreePercentageMetric.getKey(), 100 - memoryFreePercentageMetric.getValue());
        }

        Integer totalNumOfReplicas = 0;
        Integer totalNumOfInSyncReplicas = 0;
        Long totalMessagesIn = extractMinMaxDeltaValueFromMetricsMap(metricsService.getBrokerMessagesInCount(brokerNode));
        ArrayList<BrokerDetails.TopicLeaderPartitionInfo> topicLeaderPartitionInfos = new ArrayList<>();
        Collection<AggrTopicMetrics> aggrTopicMetrics = getAllTopicMetrics(timeSpan, ClientState.all.name(), securityContext);

        Set<String> activeProducerNames = new HashSet<>();
        Map<String, ConsumerGroupInfo> consumerGroupIdToGroupInfo = new HashMap<>();
        for (ProducerMetrics producerMetrics : ProducerMetrics.from(metricsService, inactiveProducerTimeoutMs,
                ClientState.active, timeSpan, authorizer,
                securityContext)) {
            activeProducerNames.add(producerMetrics.clientId());
        }
        for (ConsumerGroupInfo consumerGroupInfo : consumerGroupManagementService.consumerGroups(ClientState.all)) {
            consumerGroupIdToGroupInfo.put(consumerGroupInfo.id(), consumerGroupInfo);
        }

        List<BrokerDetails.ProducerDetail> producerDetails = new ArrayList<>();
        List<BrokerDetails.ConsumerDetail> consumerDetails = new ArrayList<>();
        for (AggrTopicMetrics aggrTopicMetric : aggrTopicMetrics) {
            for (Map.Entry<Integer, AggrTopicMetrics.WrappedTopicPartitionMetrics> partitionMetric : aggrTopicMetric.partitionMetrics().entrySet()) {
                TopicPartitionInfo topicPartitionInfo = partitionMetric.getValue().aggrTopicPartitionMetrics().partitionInfo();
                for (BrokerNode replicaNode : topicPartitionInfo.replicas()) {
                    if (replicaNode.id() == brokerId)
                        totalNumOfReplicas++;
                }
                for (BrokerNode inSyncReplica : topicPartitionInfo.isr()) {
                    if (inSyncReplica.id() == brokerId)
                        totalNumOfInSyncReplicas++;
                }

                if (topicPartitionInfo.leader().id() == brokerId) {
                    Long topicBytesInCount = aggrTopicMetric.bytesInCount();
                    Long topicBytesOutCount = aggrTopicMetric.bytesOutCount();
                    Long partitionBytesInCount = partitionMetric.getValue().aggrTopicPartitionMetrics().bytesInCount();
                    Long partitionBytesOutCount = partitionMetric.getValue().aggrTopicPartitionMetrics().bytesOutCount();
                    BrokerDetails.TopicLeaderPartitionInfo topicLeaderPartitionInfo = new BrokerDetails.TopicLeaderPartitionInfo(
                            aggrTopicMetric.name(), topicPartitionInfo.partition(), partitionBytesInCount, partitionBytesOutCount,
                            topicBytesInCount, topicBytesOutCount);
                    topicLeaderPartitionInfos.add(topicLeaderPartitionInfo);
                    for (Map.Entry<String, Long> producerIdToMessageCount : partitionMetric.getValue().producerIdToOutMessagesCount().entrySet()) {
                        String producerId = producerIdToMessageCount.getKey();
                        producerDetails.add(new BrokerDetails.ProducerDetail(producerId, activeProducerNames.contains(producerId),
                                producerIdToMessageCount.getValue()));
                    }
                    for (Map.Entry<String, Long> consumerGroupToLag : partitionMetric.getValue().consumerGroupIdToLag().entrySet()) {
                        String consumerGroupId = consumerGroupToLag.getKey();
                        if (consumerGroupIdToGroupInfo.containsKey(consumerGroupId)) {
                            String state = consumerGroupIdToGroupInfo.get(consumerGroupId).state();
                            Boolean isActive = consumerGroupIdToGroupInfo.get(consumerGroupId).active();
                            consumerDetails.add(new BrokerDetails.ConsumerDetail(consumerGroupId, isActive, state,
                                    consumerGroupToLag.getValue()));
                        }
                    }
                }
            }
        }

        return new BrokerDetails(brokerNode, totalNumOfReplicas, totalNumOfInSyncReplicas, totalMessagesIn,
                producerDetails, consumerDetails, topicLeaderPartitionInfos, cpuUsagePercentageMetric,
                diskUsagePercentageMetric, cpuLoadMetric, memoryUsagePercentageMetrics, logRetentionPeriodValue,
                logRetentionPeriodTimeUnit);
    }

    private Map<String, TopicInfo> topicInfosFromProducerMetrics(Collection<ProducerMetrics> producerMetrics) {
        long time = System.currentTimeMillis();
        final Map<String, TopicInfo> topicInfos = new HashMap<>();
        producerMetrics.forEach(x -> topicManagementService.topicInfos(x.outMessagesCount().keySet(), false)
                                                           .forEach(topicInfo -> topicInfos.put(topicInfo.name(),
                                                                                                topicInfo)));
        LOG.debug("Time taken for building topic infos from producer metrics [{} ms]",
                  (System.currentTimeMillis() - time));
        return topicInfos;
    }

    private Map<String, TopicInfo> topicInfosFromConsumerGroups(Collection<ConsumerGroupInfo> consumerGroups,
                                                                SecurityContext securityContext) {
        final Set<String> topicNames = new HashSet<>();
        consumerGroups.forEach(cg -> topicNames.addAll(cg.topicPartitionAssignments().keySet()));

        long time = System.currentTimeMillis();
        final Map<String, TopicInfo> topicInfos = new HashMap<>();
        topicManagementService.topicInfos(topicNames, false).forEach(x -> topicInfos.put(x.name(), x));
        LOG.debug("Time taken for building topic infos from consumer group metrics [{} ms]",
                  (System.currentTimeMillis() - time));

        return getFilteredTopicInfos(securityContext, topicInfos);
    }

    private Long getTopicRetentionMs(String topicName) {
        Collection<KafkaResourceConfig> kafkaResourceConfigs = resourceConfigsService.topicConfigs(Collections.singleton(topicName));
        Long topicRetentionMs = -1L;
        if (!kafkaResourceConfigs.isEmpty()) {
            for (KafkaResourceConfigEntry topicConfigEntry : kafkaResourceConfigs.iterator().next().resourceConfigs()) {
                if (topicConfigEntry.name().equals("retention.ms")) {
                    topicRetentionMs = Long.valueOf(topicConfigEntry.value());
                    break;
                }
            }
        }

        return topicRetentionMs;
    }

    private Map<String, TopicInfo> getFilteredTopicInfos(@Context SecurityContext securityContext,
                                                         Map<String, TopicInfo> topicInfos) {
        return topicInfos.entrySet().stream().
                filter(t -> SecurityUtil.authorizeTopicDescribe(authorizer, securityContext, t.getKey())).
                                 collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Double extractMaxValueFromMetricsMap(Map<MetricDescriptor, Map<Long, Double>> valueMap) {
        return extractMaxTimestampValue(extractValue(valueMap));
    }

    private Long extractMinMaxDeltaValueFromMetricsMap(Map<MetricDescriptor, Map<Long, Double>> valueMap) {
        return extractMinMaxTimestampDeltaValue(extractValueAsLong(valueMap));
    }

    private static Map<Long, Double> extractValue(Map<MetricDescriptor, Map<Long, Double>> valueMap) {
        if (valueMap == null || valueMap.isEmpty())
            return Collections.emptyMap();
        else
            return valueMap.entrySet().iterator().next().getValue();
    }

    private static Map<Long, Long> extractValueAsLong(Map<MetricDescriptor, Map<Long, Double>> map) {
        Map<Long, Long> result = new HashMap<>();
        extractValue(map).forEach((k, v) -> result.put(k, v.longValue()));
        return result;
    }
}
