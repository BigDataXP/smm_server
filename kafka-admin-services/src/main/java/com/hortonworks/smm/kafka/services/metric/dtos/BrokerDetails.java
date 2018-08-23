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
package com.hortonworks.smm.kafka.services.metric.dtos;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.hortonworks.smm.kafka.services.management.dtos.BrokerNode;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BrokerDetails {

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TopicLeaderPartitionInfo {
        @JsonProperty
        private final String topicName;

        @JsonProperty
        private final Integer partitionId;

        @JsonProperty
        private final Long bytesInCount;

        @JsonProperty
        private final Long bytesOutCount;

        @JsonProperty
        private final Long totalTopicBytesInCount;

        @JsonProperty
        private final Long totalTopicBytesOutCount;

        private TopicLeaderPartitionInfo() {
            this(null, null, null, null, null, null);
        }

        public TopicLeaderPartitionInfo(String topicName,
                                        Integer partitionId,
                                        Long bytesInCount,
                                        Long bytesOutCount,
                                        Long totalTopicBytesInCount,
                                        Long totalTopicBytesOutCount) {
            this.topicName = topicName;
            this.partitionId = partitionId;
            this.bytesInCount = bytesInCount;
            this.bytesOutCount = bytesOutCount;
            this.totalTopicBytesInCount = totalTopicBytesInCount;
            this.totalTopicBytesOutCount = totalTopicBytesOutCount;
        }

        public String topicName() {
            return topicName;
        }

        public Integer partitionId() {
            return partitionId;
        }

        public Long bytesInCount() {
            return bytesInCount;
        }

        public Long bytesOutCount() {
            return bytesOutCount;
        }

        public Long totalTopicBytesInCount() {
            return totalTopicBytesInCount;
        }

        public Long totalTopicBytesOutCount() {
            return totalTopicBytesOutCount;
        }

        @Override
        public String toString() {
            return "TopicLeaderPartitionInfo{" +
                    "topicName=" + topicName +
                    ", partitionId=" + partitionId +
                    ", bytesInCount=" + bytesInCount +
                    ", bytesOutCount=" + bytesOutCount +
                    ", totalTopicBytesInCount=" + totalTopicBytesInCount +
                    ", totalTopicBytesOutCount=" + totalTopicBytesOutCount +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TopicLeaderPartitionInfo that = (TopicLeaderPartitionInfo) o;
            return Objects.equals(topicName, that.topicName) &&
                    Objects.equals(partitionId, that.partitionId) &&
                    Objects.equals(bytesInCount, that.bytesInCount) &&
                    Objects.equals(bytesOutCount, that.bytesOutCount) &&
                    Objects.equals(totalTopicBytesInCount, that.totalTopicBytesInCount) &&
                    Objects.equals(totalTopicBytesOutCount, that.totalTopicBytesOutCount);

        }

        @Override
        public int hashCode() {
            return Objects.hash(topicName, partitionId, bytesInCount, bytesOutCount, totalTopicBytesInCount, totalTopicBytesOutCount);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ProducerDetail {

        @JsonProperty
        private final String producerName;

        @JsonProperty
        private final Boolean active;

        @JsonProperty
        private final Long messageCount;

        private ProducerDetail() {
            this(null, null, null);
        }

        public ProducerDetail(String producerName, Boolean active, Long messageCount) {
            this.producerName = producerName;
            this.active = active;
            this.messageCount = messageCount;
        }

        public String producerName() {
            return producerName;
        }

        public Boolean active() {
            return active;
        }

        public Long messageCount() {
            return messageCount;
        }

        @Override
        public String toString() {
            return "ProducerDetail{" +
                    "producerName=" + producerName +
                    ", active=" + active +
                    ", lag=" + messageCount +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProducerDetail that = (ProducerDetail) o;
            return Objects.equals(producerName, that.producerName) &&
                    Objects.equals(active, that.active) &&
                    Objects.equals(messageCount, that.messageCount);

        }

        @Override
        public int hashCode() {
            return Objects.hash(producerName, active, messageCount);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ConsumerDetail {

        @JsonProperty
        private final String consumerGroupName;

        @JsonProperty
        private final Boolean active;

        @JsonProperty
        private final String state;

        @JsonProperty
        private final Long lag;

        private ConsumerDetail() {
            this(null, false, null, null);
        }

        public ConsumerDetail(String consumerGroupName, Boolean active, String state, Long lag) {
            this.consumerGroupName = consumerGroupName;
            this.active = active;
            this.state = state;
            this.lag = lag;
        }

        public String consumerGroupName() {
            return consumerGroupName;
        }

        public Boolean active() {
            return active;
        }

        public String state() {
            return state;
        }

        public Long messageCount() {
            return lag;
        }

        @Override
        public String toString() {
            return "ConsumerDetail{" +
                    "consumerGroupName=" + consumerGroupName +
                    ", active=" + active +
                    ", state=" + state +
                    ", lag=" + lag +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ConsumerDetail that = (ConsumerDetail) o;
            return Objects.equals(consumerGroupName, that.consumerGroupName) &&
                    Objects.equals(active, that.active) &&
                    Objects.equals(state, that.state) &&
                    Objects.equals(lag, that.lag);

        }

        @Override
        public int hashCode() {
            return Objects.hash(consumerGroupName, active, state, lag);
        }
    }

    @JsonProperty
    private BrokerNode brokerNode;

    @JsonProperty
    private final Integer totalNumReplicas;

    @JsonProperty
    private final Integer totalNumInSyncReplicas;

    @JsonProperty
    private final Long totalMessages;

    @JsonProperty
    private final Collection<ProducerDetail> producerDetails;

    @JsonProperty
    private final Collection<ConsumerDetail> consumerDetails;

    @JsonProperty
    private final Collection<TopicLeaderPartitionInfo> topicLeaderPartitionInfos;

    @JsonProperty
    private final Map<Long, Double> cpuUsagePercentage;

    @JsonProperty
    private final Map<Long, Double> diskUsagePercentage;

    @JsonProperty
    private final Map<Long, Double> cpuLoad;

    @JsonProperty
    private final Map<Long, Double> memoryUsagePercentage;

    @JsonProperty
    private final Long logRetentionPeriodValue;

    @JsonProperty
    private final String logRetentionPeriodTimeUnit;

    private BrokerDetails() {
        this(null, null, null, null, null, null, null, null, null, null, null, null, null);
    }

    public BrokerDetails(BrokerNode brokerNode,
                         Integer totalNumReplicas,
                         Integer totalNumInSyncReplicas,
                         Long totalMessages,
                         Collection<ProducerDetail> producerDetails,
                         Collection<ConsumerDetail> consumerDetails,
                         Collection<TopicLeaderPartitionInfo> topicLeaderPartitionInfos,
                         Map<Long, Double> cpuUsagePercentage,
                         Map<Long, Double> diskUsagePercentage,
                         Map<Long, Double> cpuLoad,
                         Map<Long, Double> memoryUsagePercentage,
                         Long logRetentionPeriodValue,
                         String logRetentionPeriodTimeUnit) {
        this.brokerNode = brokerNode;
        this.totalNumReplicas = totalNumReplicas;
        this.totalNumInSyncReplicas = totalNumInSyncReplicas;
        this.totalMessages = totalMessages;
        this.producerDetails = producerDetails;
        this.consumerDetails = consumerDetails;
        this.topicLeaderPartitionInfos = topicLeaderPartitionInfos;
        this.cpuUsagePercentage = cpuUsagePercentage;
        this.diskUsagePercentage = diskUsagePercentage;
        this.cpuLoad = cpuLoad;
        this.memoryUsagePercentage = memoryUsagePercentage;
        this.logRetentionPeriodValue = logRetentionPeriodValue;
        this.logRetentionPeriodTimeUnit = logRetentionPeriodTimeUnit;
    }

    public BrokerNode brokerNode() {
        return brokerNode;
    }

    public Integer totalNumReplicas() {
        return totalNumReplicas;
    }

    public Integer totalNumInSyncReplicas() {
        return totalNumInSyncReplicas;
    }

    public Long totalMessages() {
        return totalMessages;
    }

    public Collection<ProducerDetail> producerDetails() {
        return producerDetails;
    }

    public Collection<ConsumerDetail> consumerDetails() {
        return consumerDetails;
    }

    public Collection<TopicLeaderPartitionInfo> topicLeaderPartitionInfo() {
        return topicLeaderPartitionInfos;
    }

    public Map<Long, Double> cpuUsagePercentage() {
        return cpuUsagePercentage;
    }

    public Map<Long, Double> diskUsagePercentage() {
        return diskUsagePercentage;
    }

    public Map<Long, Double> cpuLoad() {
        return cpuLoad;
    }

    public Map<Long, Double> memoryUsagePercentage() {
        return memoryUsagePercentage;
    }

    public Long logRetentionPeriodValue() {
        return logRetentionPeriodValue;
    }

    public String logRetentionPeriodTimeUnit() {
        return logRetentionPeriodTimeUnit;
    }

    @Override
    public String toString() {
        return "BrokerDetails{" +
                "brokerNode=" + brokerNode +
                ", totalNumReplicas=" + totalNumReplicas +
                ", totalNumInSyncReplicas=" + totalNumInSyncReplicas +
                ", totalMessages=" + totalMessages +
                ", producerDetails=" + producerDetails +
                ", consumerDetails=" + consumerDetails +
                ", topicLeaderPartitionInfos=" + topicLeaderPartitionInfos +
                ", cpuUsagePercentage=" + cpuUsagePercentage +
                ", diskUsagePercentage=" + diskUsagePercentage +
                ", cpuLoad=" + cpuLoad +
                ", memoryUsagePercentage=" + memoryUsagePercentage +
                ", logRetentionPeriodValue=" + logRetentionPeriodValue +
                ", logRetentionPeriodTimeUnit='" + logRetentionPeriodTimeUnit + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BrokerDetails that = (BrokerDetails) o;
        return Objects.equals(brokerNode, that.brokerNode) &&
                Objects.equals(totalNumReplicas, that.totalNumReplicas) &&
                Objects.equals(totalNumInSyncReplicas, that.totalNumInSyncReplicas) &&
                Objects.equals(totalMessages, that.totalMessages) &&
                Objects.equals(producerDetails, that.producerDetails) &&
                Objects.equals(consumerDetails, that.consumerDetails) &&
                Objects.equals(topicLeaderPartitionInfos, that.topicLeaderPartitionInfos) &&
                Objects.equals(cpuUsagePercentage, that.cpuUsagePercentage) &&
                Objects.equals(diskUsagePercentage, that.diskUsagePercentage) &&
                Objects.equals(cpuLoad, that.cpuLoad) &&
                Objects.equals(memoryUsagePercentage, that.memoryUsagePercentage) &&
                Objects.equals(logRetentionPeriodValue, that.logRetentionPeriodValue) &&
                Objects.equals(logRetentionPeriodTimeUnit, that.logRetentionPeriodTimeUnit);
    }

    @Override
    public int hashCode() {
        return Objects.hash(brokerNode, totalNumReplicas, totalNumInSyncReplicas, totalMessages, producerDetails,
                consumerDetails, topicLeaderPartitionInfos, cpuUsagePercentage, diskUsagePercentage, cpuLoad,
                memoryUsagePercentage, logRetentionPeriodValue, logRetentionPeriodTimeUnit);
    }
}
