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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public interface MetricDescriptorSupplier {

    // Broker metrics
    MetricDescriptor brokerBytesInCount();

    MetricDescriptor brokerBytesInRate();

    MetricDescriptor brokerBytesOutCount();

    MetricDescriptor brokerBytesOutRate();

    MetricDescriptor brokerMessagesInCount();

    MetricDescriptor brokerMessagesInRate();

    //Host metrics

    MetricDescriptor cpuIdle();

    MetricDescriptor loadFive();

    MetricDescriptor memFree();

    MetricDescriptor memTotal();

    MetricDescriptor memFreePercent();

    MetricDescriptor diskPercent();

    MetricDescriptor diskWriteBps();

    MetricDescriptor diskReadBps();

    // Cluster metrics
    MetricDescriptor clusterBytesInCount();

    MetricDescriptor clusterBytesInRate();

    MetricDescriptor clusterBytesOutCount();

    MetricDescriptor clusterBytesOutRate();

    MetricDescriptor clusterMessagesInCount();

    MetricDescriptor clusterMessagesInRate();

    MetricDescriptor clusterProduceRequestRate();

    MetricDescriptor clusterFetchRequestRate();

    MetricDescriptor clusterRequestPoolIdleUsageRate();

    // Controller metrics

    MetricDescriptor activeControllerCount();

    MetricDescriptor uncleanLeaderElectionCount();

    MetricDescriptor totalOfflinePartitionsCount();

    // topic metrics should be queried in a cluster
    MetricDescriptor topicMessagesInCount(Map<String, String> queryTags);

    MetricDescriptor producerMessagesInCount();

    MetricDescriptor producerMessagesInCount(Map<String, String> queryTags);

    MetricDescriptor topicMessagesInRate(Map<String, String> queryTags);

    MetricDescriptor topicBytesInCount(Map<String, String> queryTags);

    MetricDescriptor topicBytesInRate(Map<String, String> queryTags);

    MetricDescriptor topicBytesOutCount(Map<String, String> queryTags);

    MetricDescriptor topicBytesOutRate(Map<String, String> queryTags);

    MetricDescriptor topicFailedProduceCount(Map<String, String> queryTags);

    MetricDescriptor topicFailedFetchCount(Map<String, String> queryTags);

    // Consumer group metrics
    MetricDescriptor partitionLag(Map<String, String> queryTags);

    MetricDescriptor partitionLagRate(Map<String, String> queryTags);

    MetricDescriptor partitionCommittedOffset(Map<String, String> queryTags);

    MetricDescriptor partitionCommittedOffsetRate(Map<String, String> queryTags);

    MetricDescriptor groupLag(Map<String, String> queryTags);

    MetricDescriptor groupLagRate(Map<String, String> queryTags);

    MetricDescriptor groupCommittedOffset(Map<String, String> queryTags);

    MetricDescriptor groupCommittedOffsetRate(Map<String, String> queryTags);

    // Topic-Partition metrics
    MetricDescriptor topicPartitionMessagesInCount(Map<String, String> queryTags);

    MetricDescriptor topicPartitionBytesInCount(Map<String, String> queryTags);

    MetricDescriptor topicPartitionBytesOutCount(Map<String, String> queryTags);


    // test case descriptors
    MetricDescriptor brokerBytesInDiff();

    MetricDescriptor clusterBytesInDiff();

    MetricDescriptor clusterBytesInAvg();

    MetricDescriptor topicBytesInDiff(Map<String, String> queryTags);

    default Collection<MetricDescriptor> getBrokerMetricDescriptors() {
        return Arrays.asList(brokerBytesInCount(), brokerBytesInRate(), brokerBytesOutCount(),
                brokerBytesOutRate(), brokerMessagesInCount(), brokerMessagesInRate());
    }

    default Collection<MetricDescriptor> getBrokerHostMetricDescriptors() {
        return Arrays.asList(cpuIdle(), loadFive(), memFree(), memTotal(), diskPercent(),
                diskWriteBps(), diskReadBps());
    }

    default Collection<MetricDescriptor> getClusterMetricDescriptors() {
        return Arrays.asList(clusterBytesInCount(), clusterBytesInRate(), clusterBytesOutCount(),
                             clusterBytesOutRate(), clusterMessagesInCount(), clusterMessagesInRate(),
                            producerMessagesInCount(), clusterProduceRequestRate(), clusterFetchRequestRate(),
                            clusterRequestPoolIdleUsageRate(), activeControllerCount(), uncleanLeaderElectionCount(),
                            totalOfflinePartitionsCount());
    }

    default Collection<MetricDescriptor> getTopicMetricDescriptors(String topic) {
        Map<String, String> tags = Collections.singletonMap("topic", topic);
        return Arrays.asList(topicMessagesInCount(tags), topicMessagesInRate(tags), topicBytesInCount(tags),
            topicBytesInRate(tags), topicBytesOutCount(tags), topicBytesOutRate(tags),
            topicFailedProduceCount(tags), topicFailedFetchCount(tags));
    }

    default Collection<MetricDescriptor> getTopicPartitionMetricDescriptors(String topic) {
        Map<String, String> tags = new HashMap<>();
        tags.put("topic", topic);
        tags.put("partition", "%");
        return Arrays.asList(topicPartitionMessagesInCount(tags),
            topicPartitionBytesInCount(tags),
            topicPartitionBytesOutCount(tags));
    }

    default Collection<MetricDescriptor> getConsumerMetricDescriptors() {
        Map<String, String> partitionTags = new HashMap<>();
        partitionTags.put(AbstractMetricDescriptorSupplier.CLIENT_ID, "%");
        partitionTags.put(AbstractMetricDescriptorSupplier.CONSUMER_GROUP, "%");
        partitionTags.put(AbstractMetricDescriptorSupplier.TOPIC, "%");
        partitionTags.put(AbstractMetricDescriptorSupplier.PARTITION_NUMBER, "%");

        Map<String, String> groupTags = new HashMap<>();
        groupTags.put(AbstractMetricDescriptorSupplier.CONSUMER_GROUP, "%");

        return Arrays.asList(partitionLag(partitionTags),
            partitionLagRate(partitionTags),
            partitionCommittedOffset(partitionTags),
            partitionCommittedOffsetRate(partitionTags),
            groupLag(groupTags),
            groupLagRate(groupTags),
            groupCommittedOffset(groupTags),
            groupCommittedOffsetRate(groupTags));
    }

}
