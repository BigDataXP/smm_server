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

import java.util.Map;

public class DummyMetricDescriptorSupplier extends AbstractMetricDescriptorSupplier {
    @Override
    public MetricDescriptor brokerBytesInCount() {
        return null;
    }

    @Override
    public MetricDescriptor brokerBytesInRate() {
        return null;
    }

    @Override
    public MetricDescriptor brokerBytesOutCount() {
        return null;
    }

    @Override
    public MetricDescriptor brokerBytesOutRate() {
        return null;
    }

    @Override
    public MetricDescriptor brokerMessagesInCount() {
        return null;
    }

    @Override
    public MetricDescriptor brokerMessagesInRate() {
        return null;
    }

    @Override
    public MetricDescriptor cpuIdle() {
        return null;
    }

    @Override
    public MetricDescriptor loadFive() {
        return null;
    }

    @Override
    public MetricDescriptor memFree() {
        return null;
    }

    @Override
    public MetricDescriptor memTotal() {
        return null;
    }

    @Override
    public MetricDescriptor memFreePercent() {
        return null;
    }

    @Override
    public MetricDescriptor diskPercent() {
        return null;
    }

    @Override
    public MetricDescriptor diskWriteBps() {
        return null;
    }

    @Override
    public MetricDescriptor diskReadBps() {
        return null;
    }

    @Override
    public MetricDescriptor clusterBytesInCount() {
        return null;
    }

    @Override
    public MetricDescriptor clusterBytesInRate() {
        return null;
    }

    @Override
    public MetricDescriptor clusterBytesOutCount() {
        return null;
    }

    @Override
    public MetricDescriptor clusterBytesOutRate() {
        return null;
    }

    @Override
    public MetricDescriptor clusterMessagesInCount() {
        return null;
    }

    @Override
    public MetricDescriptor clusterMessagesInRate() {
        return null;
    }

    @Override
    public MetricDescriptor clusterProduceRequestRate() {
        return null;
    }

    @Override
    public MetricDescriptor clusterFetchRequestRate() {
        return null;
    }

    @Override
    public MetricDescriptor clusterRequestPoolIdleUsageRate() {
        return null;
    }

    @Override
    public MetricDescriptor activeControllerCount() {
        return null;
    }

    @Override
    public MetricDescriptor uncleanLeaderElectionCount() {
        return null;
    }

    @Override
    public MetricDescriptor totalOfflinePartitionsCount() {
        return null;
    }

    @Override
    public MetricDescriptor topicMessagesInCount(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor producerMessagesInCount() {
        return null;
    }

    @Override
    public MetricDescriptor producerMessagesInCount(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicMessagesInRate(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicBytesInCount(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicBytesInRate(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicBytesOutCount(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicBytesOutRate(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicFailedProduceCount(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicFailedFetchCount(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor partitionLag(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor partitionLagRate(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor partitionCommittedOffset(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor partitionCommittedOffsetRate(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor groupLag(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor groupLagRate(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor groupCommittedOffset(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor groupCommittedOffsetRate(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicPartitionMessagesInCount(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicPartitionBytesInCount(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor topicPartitionBytesOutCount(Map<String, String> queryTags) {
        return null;
    }

    @Override
    public MetricDescriptor brokerBytesInDiff() {
        return null;
    }

    @Override
    public MetricDescriptor clusterBytesInDiff() {
        return null;
    }

    @Override
    public MetricDescriptor clusterBytesInAvg() {
        return null;
    }

    @Override
    public MetricDescriptor topicBytesInDiff(Map<String, String> queryTags) {
        return null;
    }
}
