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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.smm.kafka.services.Service;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;
import com.hortonworks.smm.kafka.services.metric.cache.MetricsCache;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier.CLIENT_ID;
import static com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier.CONSUMER_GROUP;
import static com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier.PARTITION_NUMBER;
import static com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier.TOPIC;

@Singleton
public class MetricsService implements Service {

    public static final String METRICS_IMPL_CLASSNAME_KEY = "metrics.fetcher.class";
    public static final String WILD_CARD = "%";

    private MetricsCache metricsCache;
    private MetricDescriptorSupplier supplier;
    private MetricsFetcher metricsFetcher;

    // for testing
    protected MetricsService() {
    }

    @Inject
    public MetricsService(MetricsFetcher metricsFetcher, MetricsCache metricsCache) {
        this.metricsFetcher = metricsFetcher;
        this.supplier = metricsFetcher.getMetricDescriptorSupplier();
        this.metricsCache = metricsCache;
    }

    public MetricsFetcher getMetricsFetcher() {
        return metricsFetcher;
    }

    public MetricDescriptorSupplier getSupplier() {
        return supplier;
    }

    // Broker metrics
    public Map<MetricDescriptor, Map<Long, Double>> getBrokerBytesInCount(BrokerNode node) {
        return getBrokerBytesInCount(TimeSpan.EMPTY, node);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerBytesInCount(TimeSpan timeSpan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timeSpan, supplier.brokerBytesInCount());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerBytesInRate(TimeSpan timespan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timespan, supplier.brokerBytesInRate());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerBytesOutCount(BrokerNode node) {
        return getBrokerBytesOutCount(TimeSpan.EMPTY, node);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerBytesOutCount(TimeSpan timespan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timespan, supplier.brokerBytesOutCount());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerBytesOutRate(TimeSpan timespan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timespan, supplier.brokerBytesOutRate());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerMessagesInCount(BrokerNode node) {
        return getBrokerMessagesInCount(TimeSpan.EMPTY, node);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerMessagesInCount(TimeSpan timespan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timespan, supplier.brokerMessagesInCount());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getBrokerMessagesInRate(TimeSpan timespan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timespan, supplier.brokerMessagesInRate());
    }

    // cluster metrics
    public Map<MetricDescriptor, Map<Long, Double>> getClusterBytesInCount() {
        return getClusterBytesInCount(TimeSpan.EMPTY);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterBytesInCount(TimeSpan timespan) {
        return metricsCache.getClusterMetrics(timespan, supplier.clusterBytesInCount());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterBytesInRate(TimeSpan timespan) {
        return metricsCache.getClusterMetrics(timespan, supplier.clusterBytesInRate());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterBytesOutCount() {
        return getClusterBytesOutCount(TimeSpan.EMPTY);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterBytesOutCount(TimeSpan timespan) {
        return metricsCache.getClusterMetrics(timespan, supplier.clusterBytesOutCount());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterBytesOutRate(TimeSpan timespan) {
        return metricsCache.getClusterMetrics(timespan, supplier.clusterBytesOutRate());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterMessagesInCount() {
        return getClusterMessagesInCount(TimeSpan.EMPTY);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterMessagesInCount(TimeSpan timespan) {
        return metricsCache.getClusterMetrics(timespan, supplier.clusterMessagesInCount());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getClusterMessagesInRate(TimeSpan timespan) {
        return metricsCache.getClusterMetrics(timespan, supplier.clusterMessagesInRate());
    }

    public Map<MetricDescriptor, Map<Long,Double>> getClusterProduceRequestRate(TimeSpan timeSpan) {
        return metricsCache.getClusterMetrics(timeSpan, supplier.clusterProduceRequestRate());
    }

    public Map<MetricDescriptor, Map<Long,Double>> getClusterFetchRequestRate(TimeSpan timeSpan) {
        return metricsCache.getClusterMetrics(timeSpan, supplier.clusterFetchRequestRate());
    }

    public Map<MetricDescriptor, Map<Long,Double>> getClusterRequestPoolIdleUsageRate(TimeSpan timeSpan) {
        return metricsCache.getClusterMetrics(timeSpan, supplier.clusterRequestPoolIdleUsageRate());
    }

    // Controller metrics

    public Map<MetricDescriptor, Map<Long,Double>> getActiveControllerCount(TimeSpan timeSpan) {
        return metricsCache.getClusterMetrics(timeSpan, supplier.activeControllerCount());
    }

    public Map<MetricDescriptor, Map<Long,Double>> getUncleanLeaderElectionCount(TimeSpan timeSpan) {
        return metricsCache.getClusterMetrics(timeSpan, supplier.uncleanLeaderElectionCount());
    }

    public Map<MetricDescriptor, Map<Long,Double>> getTotalOfflinePartitionsCount(TimeSpan timeSpan) {
        return metricsCache.getClusterMetrics(timeSpan, supplier.totalOfflinePartitionsCount());
    }

    // Topic metrics
    public Map<MetricDescriptor, Map<Long, Double>> getTopicMessagesInCount(String topic) {
        return getTopicMessagesInCount(TimeSpan.EMPTY, topic);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicMessagesInCount(TimeSpan timespan, String topic) {
        return metricsCache.getClusterMetrics(timespan,
                                              supplier.topicMessagesInCount(Collections.singletonMap("topic", topic)));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicMessagesInRate(TimeSpan timespan, String topic) {
        return metricsCache.getClusterMetrics(timespan,
                                              supplier.topicMessagesInRate(Collections.singletonMap("topic", topic)));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicBytesInCount(String topic) {
        return getTopicBytesInCount(TimeSpan.EMPTY, topic);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicBytesInCount(TimeSpan timespan, String topic) {
        return metricsCache.getClusterMetrics(timespan,
                                              supplier.topicBytesInCount(Collections.singletonMap("topic", topic)));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicBytesInRate(TimeSpan timespan, String topic) {
        return metricsCache.getClusterMetrics(timespan,
                                              supplier.topicBytesInRate(Collections.singletonMap("topic", topic)));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicBytesOutCount(String topic) {
        return getTopicBytesOutCount(TimeSpan.EMPTY, topic);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicBytesOutCount(TimeSpan timespan, String topic) {
        return metricsCache.getClusterMetrics(timespan,
                                              supplier.topicBytesOutCount(Collections.singletonMap("topic", topic)));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicBytesOutRate(TimeSpan timespan, String topic) {
        return metricsCache.getClusterMetrics(timespan,
                                              supplier.topicBytesOutRate(Collections.singletonMap("topic", topic)));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicFailedProduceCount(String topic) {
        return metricsCache.getClusterMetrics(TimeSpan.EMPTY, supplier.topicFailedProduceCount(
                Collections.singletonMap("topic", topic)));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicFailedFetchCount(String topic) {
        return metricsCache.getClusterMetrics(TimeSpan.EMPTY,
                                              supplier.topicFailedFetchCount(Collections.singletonMap("topic", topic)));
    }

    // producer metrics api
    public Map<MetricDescriptor, Map<Long, Double>> getAllProducerInMessagesCount(TimeSpan timeSpan) {
        return metricsCache.getClusterMetrics(timeSpan, supplier.producerMessagesInCount());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getProducerInMessagesCount(TimeSpan timeSpan, String clientId) {
        Map<String, String> queryTags = new HashMap<>();
        queryTags.put("clientId", clientId);
        queryTags.put("topic", WILD_CARD);
        queryTags.put("partition", WILD_CARD);
        return metricsCache.getClusterMetrics(timeSpan, supplier.producerMessagesInCount(queryTags));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getPartitionLagForGroup(TimeSpan timespan, String groupId) {
        Map<MetricDescriptor, Map<Long, Double>> partitionLag =
                metricsCache.getClusterMetrics(timespan, supplier.partitionLag(getTagsForConsumerGroup(groupId)));
        return partitionLag;
    }

    public Map<MetricDescriptor, Map<Long, Double>> getPartitionLagRateForGroup(TimeSpan timespan, String groupId) {
        Map<MetricDescriptor, Map<Long, Double>> partitionLagRate =
                metricsCache.getClusterMetrics(timespan, supplier.partitionLagRate(getTagsForConsumerGroup(groupId)));
        return partitionLagRate;
    }

    public Map<MetricDescriptor, Map<Long, Double>> getPartitionCommittedOffsetsForGroup(TimeSpan timespan, String groupId) {
        Map<MetricDescriptor, Map<Long, Double>> partitionCommittedOffsets =
                metricsCache.getClusterMetrics(timespan, supplier.partitionCommittedOffset(getTagsForConsumerGroup(groupId)));
        return partitionCommittedOffsets;
    }

    public Map<MetricDescriptor, Map<Long, Double>> getPartitionCommittedOffsetsRateForGroup(TimeSpan timespan, String groupId) {
        Map<MetricDescriptor, Map<Long, Double>> partitionCommittedOffsetsRate =
                metricsCache.getClusterMetrics(timespan, supplier.partitionCommittedOffsetRate(getTagsForConsumerGroup(groupId)));
        return partitionCommittedOffsetsRate;
    }

    public Map<MetricDescriptor, Map<Long, Double>> getCumulativeLag(TimeSpan timespan, String groupId) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(CONSUMER_GROUP, groupId);
        List<MetricDescriptor> descriptors = Collections.singletonList(supplier.groupLag(tags));
        return metricsCache.getClusterMetrics(timespan, descriptors);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getCumulativeLagRate(TimeSpan timespan, String groupId) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(CONSUMER_GROUP, groupId);
        List<MetricDescriptor> descriptors = Collections.singletonList(supplier.groupLagRate(tags));
        return metricsCache.getClusterMetrics(timespan, descriptors);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getCumulativeCommittedOffsets(TimeSpan timespan, String groupId) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(CONSUMER_GROUP, groupId);
        List<MetricDescriptor> descriptors = Collections.singletonList(supplier.groupCommittedOffset(tags));
        return metricsCache.getClusterMetrics(timespan, descriptors);
    }

    public Map<MetricDescriptor, Map<Long, Double>> getCumulativeCommittedOffsetRate(TimeSpan timespan, String groupId) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(CONSUMER_GROUP, groupId);
        List<MetricDescriptor> descriptors = Collections.singletonList(supplier.groupCommittedOffsetRate(tags));
        return metricsCache.getClusterMetrics(timespan, descriptors);
    }

    private Map<String,String> getTagsForConsumerGroup(String groupId) {
        Map<String, String> tags = new LinkedHashMap<>();
        tags.put(CLIENT_ID, WILD_CARD);
        tags.put(CONSUMER_GROUP, groupId);
        tags.put(TOPIC, WILD_CARD);
        tags.put(PARTITION_NUMBER, WILD_CARD);
        return  tags;
    }

    @Override
    public void close() throws Exception {
        metricsFetcher.close();
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicPartitionMessagesInCount(TimeSpan timeSpan,
                                                                                     String topic) {
        Map<String, String> queryTags = getTopicPartitionTags(topic, WILD_CARD);
        return metricsCache.getClusterMetrics(timeSpan, supplier.topicPartitionMessagesInCount(queryTags));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicPartitionMessagesInCount(TimeSpan timeSpan,
                                                                                     String topic,
                                                                                     int partition) {
        Map<String, String> queryTags = getTopicPartitionTags(topic, String.valueOf(partition));
        return metricsCache.getClusterMetrics(timeSpan, supplier.topicPartitionMessagesInCount(queryTags));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicPartitionBytesInCount(TimeSpan timeSpan,
                                                                                  String topic) {
        Map<String, String> queryTags = getTopicPartitionTags(topic, WILD_CARD);
        return metricsCache.getClusterMetrics(timeSpan, supplier.topicPartitionBytesInCount(queryTags));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicPartitionBytesInCount(TimeSpan timeSpan,
                                                                                  String topic,
                                                                                  int partition) {
        Map<String, String> queryTags = getTopicPartitionTags(topic, String.valueOf(partition));
        return metricsCache.getClusterMetrics(timeSpan, supplier.topicPartitionBytesInCount(queryTags));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicPartitionBytesOutCount(TimeSpan timeSpan,
                                                                                   String topic) {
        Map<String, String> queryTags = getTopicPartitionTags(topic, WILD_CARD);
        return metricsCache.getClusterMetrics(timeSpan, supplier.topicPartitionBytesOutCount(queryTags));
    }

    public Map<MetricDescriptor, Map<Long, Double>> getTopicPartitionBytesOutCount(TimeSpan timeSpan,
                                                                                   String topic,
                                                                                   int partition) {
        Map<String, String> queryTags = getTopicPartitionTags(topic, String.valueOf(partition));
        return metricsCache.getClusterMetrics(timeSpan, supplier.topicPartitionBytesOutCount(queryTags));
    }

    private Map<String, String> getTopicPartitionTags(String topic, String partition) {
        Map<String, String> queryTags = new HashMap<>();
        queryTags.put("topic", topic);
        queryTags.put("partition", partition);
        return queryTags;
    }

    public Map<MetricDescriptor, Map<Long, Double>> getCpuIdleMetrics(TimeSpan timeSpan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timeSpan, supplier.cpuIdle());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getLoadFiveMetrics(TimeSpan timeSpan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timeSpan, supplier.loadFive());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getMemFreePercentMetrics(TimeSpan timeSpan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timeSpan, supplier.memFreePercent());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getDiskPercentMetrics(TimeSpan timeSpan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timeSpan, supplier.diskPercent());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getDiskWriteBpsMetrics(TimeSpan timeSpan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timeSpan, supplier.diskWriteBps());
    }

    public Map<MetricDescriptor, Map<Long, Double>> getDiskReadBpsMetrics(TimeSpan timeSpan, BrokerNode node) {
        return metricsCache.getBrokerMetrics(node, timeSpan, supplier.diskReadBps());
    }

    public boolean emitMetrics(Map<MetricDescriptor, Long> metrics) {
        return getMetricsFetcher().emitMetrics(metrics);
    }
}
