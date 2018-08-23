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
package com.hortonworks.smm.kafka.services.metric.ams;

import com.hortonworks.smm.kafka.services.metric.AbstractMetricDescriptorSupplier;
import com.hortonworks.smm.kafka.services.metric.AggregateFunction;
import com.hortonworks.smm.kafka.services.metric.MetricDescriptor;
import com.hortonworks.smm.kafka.services.metric.MetricName;
import com.hortonworks.smm.kafka.services.metric.PostProcessFunction;

import java.util.HashMap;
import java.util.Map;

public class AMSMetricDescriptorSupplier extends AbstractMetricDescriptorSupplier {

    @Override
    public MetricDescriptor brokerBytesInCount() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor brokerBytesInRate() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().withPostProcessFunction(PostProcessFunction.RATE).build(name);
    }

    @Override
    public MetricDescriptor brokerBytesOutCount() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_OUT_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor brokerBytesOutRate() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_OUT_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().withPostProcessFunction(PostProcessFunction.RATE).build(name);
    }

    @Override
    public MetricDescriptor brokerMessagesInCount() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, MESSAGES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor brokerMessagesInRate() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, MESSAGES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().withPostProcessFunction(PostProcessFunction.RATE).build(name);
    }

    @Override
    public MetricDescriptor cpuIdle() {
        MetricName name = MetricName.from(EMPTY_ATTR, EMPTY_ATTR, CPU_IDLE, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor loadFive() {
        MetricName name = MetricName.from(EMPTY_ATTR, EMPTY_ATTR, LOAD_FIVE, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor memFree() {
        MetricName name = MetricName.from(EMPTY_ATTR, EMPTY_ATTR, MEM_FREE, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor memTotal() {
        MetricName name = MetricName.from(EMPTY_ATTR, EMPTY_ATTR, MEM_TOTAL, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor memFreePercent() {
        MetricName name = MetricName.from(EMPTY_ATTR, EMPTY_ATTR, MEM_FREE_PERCENT, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor diskPercent() {
        MetricName name = MetricName.from(EMPTY_ATTR, EMPTY_ATTR, DISK_PERCENT, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor diskWriteBps() {
        MetricName name = MetricName.from(EMPTY_ATTR, EMPTY_ATTR, DISK_WRITE_BPS, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor diskReadBps() {
        MetricName name = MetricName.from(EMPTY_ATTR, EMPTY_ATTR, DISK_READ_BPS, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor clusterBytesInCount() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().withAggregationFunction(AggregateFunction.SUM).build(name);
    }

    @Override
    public MetricDescriptor clusterBytesInRate() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withPostProcessFunction(PostProcessFunction.RATE).build(name);
    }

    @Override
    public MetricDescriptor clusterBytesOutCount() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_OUT_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().withAggregationFunction(AggregateFunction.SUM).build(name);
    }

    @Override
    public MetricDescriptor clusterBytesOutRate() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_OUT_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withPostProcessFunction(PostProcessFunction.RATE).build(name);
    }

    @Override
    public MetricDescriptor clusterMessagesInCount() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, MESSAGES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().withAggregationFunction(AggregateFunction.SUM).build(name);
    }

    @Override
    public MetricDescriptor clusterMessagesInRate() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, MESSAGES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withPostProcessFunction(PostProcessFunction.RATE)
                .build(name);
    }

    @Override
    public MetricDescriptor clusterProduceRequestRate() {
       MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, TOTAL_PRODUCE_REQUESTS_PER_SEC, FIVE_MINUTE_RATE, EMPTY_TAG);
       return MetricDescriptor.newBuilder()
               .withAggregationFunction(AggregateFunction.SUM)
               .build(name);
    }

    @Override
    public MetricDescriptor clusterFetchRequestRate() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, TOTAL_FETCH_REQUESTS_PER_SEC, FIVE_MINUTE_RATE, EMPTY_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .build(name);
    }

    @Override
    public MetricDescriptor clusterRequestPoolIdleUsageRate() {
        MetricName name = MetricName.from(KAFKA_SERVER, KAFKA_REQUEST_HANDLER_POOL, REQUEST_HANDLER_AVG_IDLE_PERCENT, FIVE_MINUTE_RATE, EMPTY_TAG);
        return MetricDescriptor.newBuilder().build(name);
    }

    @Override
    public MetricDescriptor activeControllerCount() {
        MetricName name = MetricName.from(KAFKA_CONTROLLER, KAFKA_CONTROLLER_TYPE, ACTIVE_CONTROLLER_COUNT, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .build(name);
    }

    @Override
    public MetricDescriptor uncleanLeaderElectionCount() {
        MetricName name = MetricName.from(KAFKA_CONTROLLER, CONTROLLER_STATS, UNCLEAN_LEADER_ELECTIONS_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .build(name);
    }

    @Override
    public MetricDescriptor totalOfflinePartitionsCount() {
        MetricName name = MetricName.from(KAFKA_CONTROLLER, KAFKA_CONTROLLER_TYPE, OFFLINE_PARTITIONS_COUNT, EMPTY_ATTR, EMPTY_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .build(name);
    }


    @Override
    public MetricDescriptor topicMessagesInCount(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, MESSAGES_IN_PER_SEC, COUNT, TOPIC_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withQueryTags(queryTags)
                .build(name);
    }

    @Override
    public MetricDescriptor producerMessagesInCount() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_CLIENT_METRICS, MESSAGES_IN_PER_SEC, COUNT,
                PRODUCER_TP_TAG);
        Map<String, String> queryTags = new HashMap<>();
        for (String tag : PRODUCER_TP_TAG) {
            queryTags.put(tag, "%");
        }

        return MetricDescriptor.newBuilder()
                .withQueryTags(queryTags)
                .build(name);
    }

    @Override
    public MetricDescriptor producerMessagesInCount(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_CLIENT_METRICS, MESSAGES_IN_PER_SEC, COUNT,
                PRODUCER_TP_TAG);
        return MetricDescriptor.newBuilder()
                .withQueryTags(queryTags)
                .build(name);
    }

    @Override
    public MetricDescriptor topicMessagesInRate(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, MESSAGES_IN_PER_SEC, COUNT, TOPIC_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withPostProcessFunction(PostProcessFunction.RATE)
                .withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor topicBytesInCount(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, TOPIC_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withQueryTags(queryTags)
                .build(name);
    }

    @Override
    public MetricDescriptor topicBytesInRate(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, TOPIC_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withPostProcessFunction(PostProcessFunction.RATE)
                .withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor topicBytesOutCount(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_OUT_PER_SEC, COUNT, TOPIC_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor topicBytesOutRate(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_OUT_PER_SEC, COUNT, TOPIC_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withPostProcessFunction(PostProcessFunction.RATE)
                .withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor topicFailedProduceCount(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, FAILED_PRODUCE_REQUESTS_PER_SEC, COUNT, TOPIC_TAG);
        return MetricDescriptor.newBuilder()
               .withAggregationFunction(AggregateFunction.SUM)
               .withQueryTags(queryTags)
               .build(name);
    }

    @Override
    public MetricDescriptor topicFailedFetchCount(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, FAILED_FETCH_REQUESTS_PER_SEC, COUNT, TOPIC_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withQueryTags(queryTags)
                .build(name);
    }

    @Override
    public MetricDescriptor partitionLag(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, CONSUMER_GROUP_METRICS, LAG, EMPTY_ATTR, CONSUMER_GROUP_TAG);
        return MetricDescriptor.newBuilder().withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor partitionLagRate(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, CONSUMER_GROUP_METRICS, LAG, EMPTY_ATTR, CONSUMER_GROUP_TAG);
        return MetricDescriptor.newBuilder()
            .withAggregationFunction(AggregateFunction.SUM)
            .withPostProcessFunction(PostProcessFunction.RATE)
            .withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor partitionCommittedOffset(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, CONSUMER_GROUP_METRICS, COMMITTED_OFFSET, EMPTY_ATTR, CONSUMER_GROUP_TAG);
        return MetricDescriptor.newBuilder().withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor partitionCommittedOffsetRate(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, CONSUMER_GROUP_METRICS, COMMITTED_OFFSET, EMPTY_ATTR, CONSUMER_GROUP_TAG);
        return MetricDescriptor.newBuilder()
            .withAggregationFunction(AggregateFunction.SUM)
            .withPostProcessFunction(PostProcessFunction.RATE)
            .withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor groupLag(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, CONSUMER_GROUP_METRICS, LAG, EMPTY_ATTR, GROUP_TAG);
        return MetricDescriptor.newBuilder().withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor groupLagRate(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, CONSUMER_GROUP_METRICS, LAG, EMPTY_ATTR, GROUP_TAG);
        return MetricDescriptor.newBuilder()
            .withAggregationFunction(AggregateFunction.SUM)
            .withPostProcessFunction(PostProcessFunction.RATE)
            .withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor groupCommittedOffset(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, CONSUMER_GROUP_METRICS, COMMITTED_OFFSET, EMPTY_ATTR, GROUP_TAG);
        return MetricDescriptor.newBuilder().withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor groupCommittedOffsetRate(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, CONSUMER_GROUP_METRICS, COMMITTED_OFFSET, EMPTY_ATTR, GROUP_TAG);
        return MetricDescriptor.newBuilder()
            .withAggregationFunction(AggregateFunction.SUM)
            .withPostProcessFunction(PostProcessFunction.RATE)
            .withQueryTags(queryTags).build(name);
    }

    @Override
    public MetricDescriptor topicPartitionMessagesInCount(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, MESSAGES_IN_PER_SEC, COUNT,
                TOPIC_PARTITION_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withQueryTags(queryTags)
                .build(name);
    }

    @Override
    public MetricDescriptor topicPartitionBytesInCount(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT,
                TOPIC_PARTITION_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withQueryTags(queryTags)
                .build(name);
    }

    @Override
    public MetricDescriptor topicPartitionBytesOutCount(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_OUT_PER_SEC, COUNT,
                TOPIC_PARTITION_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withQueryTags(queryTags)
                .build(name);
    }


    // test case descriptors
    @Override
    public MetricDescriptor brokerBytesInDiff() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder().withPostProcessFunction(PostProcessFunction.DIFF).build(name);
    }

    @Override
    public MetricDescriptor clusterBytesInDiff() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withPostProcessFunction(PostProcessFunction.DIFF).build(name);
    }

    @Override
    public MetricDescriptor clusterBytesInAvg() {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, EMPTY_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.AVG)
                .build(name);
    }

    @Override
    public MetricDescriptor topicBytesInDiff(Map<String, String> queryTags) {
        MetricName name = MetricName.from(KAFKA_SERVER, BROKER_TOPIC_METRICS, BYTES_IN_PER_SEC, COUNT, TOPIC_TAG);
        return MetricDescriptor.newBuilder()
                .withAggregationFunction(AggregateFunction.SUM)
                .withPostProcessFunction(PostProcessFunction.DIFF)
                .withQueryTags(queryTags)
                .build(name);
    }

}
