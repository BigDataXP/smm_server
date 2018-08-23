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

import com.hortonworks.smm.kafka.common.config.KafkaAdminClientConfig;
import com.hortonworks.smm.kafka.common.config.KafkaConsumerConfig;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerGroupInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.PartitionAssignment;
import kafka.common.OffsetAndMetadata;
import kafka.coordinator.group.BaseKey;
import kafka.coordinator.group.GroupMetadata;
import kafka.coordinator.group.GroupMetadataKey;
import kafka.coordinator.group.GroupMetadataManager;
import kafka.coordinator.group.GroupOverview;
import kafka.coordinator.group.GroupSummary;
import kafka.coordinator.group.GroupTopicPartition;
import kafka.coordinator.group.MemberSummary;
import kafka.coordinator.group.OffsetKey;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hortonworks.smm.kafka.services.clients.dtos.PartitionAssignment.DEFAULT_CLIENT_ID;
import static com.hortonworks.smm.kafka.services.clients.dtos.PartitionAssignment.DEFAULT_CONSUMER_ID;
import static com.hortonworks.smm.kafka.services.clients.dtos.PartitionAssignment.DEFAULT_HOST;
import static com.hortonworks.smm.kafka.services.clients.dtos.PartitionAssignment.DEFAULT_OFFSET;
import static org.apache.kafka.common.internals.Topic.GROUP_METADATA_TOPIC_NAME;

class ConsumerGroupFetcherTask implements Runnable, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerGroupFetcherTask.class);

    private final AtomicBoolean refreshNow = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<String, Map<TopicPartition, OffsetWithMetadata>> commitOffsets = new ConcurrentHashMap<>();
    private final Map<String, ConsumerGroupInfo> consumerGroupInfos = new ConcurrentHashMap<>();
    private final Map<String, GroupMetadata> groups = new HashMap<>();
    private final Supplier<Set<TopicPartition>> topicPartitionsSupplier;

    private KafkaConsumer<ByteBuffer, ByteBuffer> kafkaConsumer;
    private Map<TopicPartition, Long> logEndOffsets;
    private long commitOffsetRefreshMs;
    private kafka.admin.AdminClient scalaAdminClient;
    private Long inactiveGroupTimeoutMs;
    private boolean seekToBeginning = true;

    public ConsumerGroupFetcherTask(KafkaConsumerConfig consumerConfig,
                                    KafkaAdminClientConfig adminClientConfig,
                                    long inactiveGroupTimeoutMs,
                                    Supplier<Set<TopicPartition>> topicPartitionsSupplier) {
        this.topicPartitionsSupplier = topicPartitionsSupplier;
        this.inactiveGroupTimeoutMs = inactiveGroupTimeoutMs;

        Properties props = new Properties();
        adminClientConfig.getConfig().entrySet().forEach(adminClientConfigEntry -> {
            props.put(adminClientConfigEntry.getKey(), adminClientConfigEntry.getValue().toString());
                });
        scalaAdminClient = kafka.admin.AdminClient.create(props);
        createKafkaConsumer(consumerConfig);
    }

    private void createKafkaConsumer(KafkaConsumerConfig consumerConfig) {
        Map<String, Object> config = consumerConfig.getConfig();
        Map<String, Object> props = new HashMap<>(config);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.ByteBufferDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.ByteBufferDeserializer");

        String groupId = ConsumerGroupsService.CG_FETCHER_GROUP_ID_PREFIX;
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, groupId);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Since we are using assign mode and reading the offsets from beginning on every restart,
        //we can disable auto commit.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        commitOffsetRefreshMs = Long.parseLong(config.getOrDefault(
                ConsumerGroupsService.COMMIT_OFFSETS_REFRESH_INTERVAL_MS,
                ConsumerGroupsService.DEFAULT_COMMIT_OFFSETS_REFRESH_INTERVAL_MS).toString());

        kafkaConsumer = new KafkaConsumer<>(props);
        refreshLEOsForAllPartitions();
    }

    void waitTillRefresh() throws Exception {
        refreshNow.set(true);
        while (!refreshNow.compareAndSet(false, false)) {
            Thread.sleep(100L);
        }
    }

    public void run() {
        try {
            Long leosRefreshTime = System.currentTimeMillis();
            while (!closed.get()) {
                long startTime = System.currentTimeMillis();

                try {
                    if (refreshNow.get() || startTime - leosRefreshTime > commitOffsetRefreshMs) {
                        refreshLEOsForAllPartitions();
                        updateConsumerGroupsWithLEOs();
                        leosRefreshTime = System.currentTimeMillis();
                        refreshNow.set(false);
                    }

                    if (seekToBeginning) {
                        Collection<TopicPartition> partitions = consumerOffsetTopicPartitions();
                        kafkaConsumer.assign(partitions);
                        kafkaConsumer.seekToBeginning(Collections.EMPTY_LIST);
                        seekToBeginning = false;
                        LOG.info("seeking to the beginning of the consumer offsets topic, partition assignment : {}" , kafkaConsumer.assignment());
                    } else {
                        ConsumerRecords<ByteBuffer, ByteBuffer> consumerRecords = kafkaConsumer.poll(5000);
                        Set<String> addedGroups = new HashSet<>();
                        Set<String> removedGroups = new HashSet<>();
                        for (ConsumerRecord<ByteBuffer, ByteBuffer> consumerRecord : consumerRecords) {
                            BaseKey baseKey = GroupMetadataManager.readMessageKey(consumerRecord.key());
                            ByteBuffer value = consumerRecord.value();
                            if (baseKey instanceof GroupMetadataKey) {
                                handleGroupMetadataKey(addedGroups, removedGroups, (GroupMetadataKey) baseKey, value);
                            } else if (baseKey instanceof OffsetKey) {
                                handleOffsetKey((OffsetKey) baseKey, value);
                            }

                            removedGroups.forEach(groupId -> {
                                commitOffsets.remove(groupId);
                                consumerGroupInfos.remove(groupId);
                            });
                            Map<String, ConsumerGroupInfo> addedGroupInfos = buildConsumerGroups(addedGroups);
                            consumerGroupInfos.putAll(addedGroupInfos);
                        }
                        if (!addedGroups.isEmpty() || !removedGroups.isEmpty())
                            LOG.debug("## received consumer group updates added: [{}], removed: [{}], total: [{}] took time [{}] msecs",
                                      addedGroups, removedGroups, consumerGroupInfos.size(), (System.currentTimeMillis() - startTime));
                    }

                } catch (Exception e) {
                    if(!(e instanceof WakeupException)) {
                        LOG.warn("Encountered error while fetching and build consumer groups", e);
                    }
                }
            }
        } catch (WakeupException e) {
            if (closed.get()) {
                LOG.error("Error encountered in fetching consumer group offsets", e);
            } else {
                throw e;
            }
        } finally {
            Utils.closeQuietly(kafkaConsumer, "kafka consumer for fetching consumer-groups");
            // Release the thread which may be waiting on refresh
            refreshNow.set(false);
        }
    }

    private Collection<TopicPartition> consumerOffsetTopicPartitions() {
        List<PartitionInfo> partitions = kafkaConsumer.partitionsFor(GROUP_METADATA_TOPIC_NAME);
        return partitions.stream().map(p -> new TopicPartition(p.topic(), p.partition())).collect(Collectors.toList());
    }

    private void updateConsumerGroupsWithLEOs() {
        // get consumer groups with manual partitions
        for (GroupOverview groupOverview : JavaConversions
                .asJavaCollection(scalaAdminClient.listAllGroupsFlattened())) {
            String groupId = groupOverview.groupId();
            if (!isInternalGroup(groupId)) {
                consumerGroupInfos.computeIfAbsent(groupId,
                                                   x -> new ConsumerGroupInfo(x, "Empty"));
            }
        }

        for (ConsumerGroupInfo consumerGroupInfo : consumerGroupInfos.values()) {
            String groupId = consumerGroupInfo.id();

            Map<String, Map<Integer, PartitionAssignment>> partitionAssignments =
                    consumerGroupInfo.topicPartitionAssignments();

            if (partitionAssignments == null || partitionAssignments.isEmpty()) {
                // it happens incase of manual partition assignments
                Map<String, Map<Integer, PartitionAssignment>> newAssignments = new HashMap<>();
                commitOffsets.getOrDefault(groupId, Collections.emptyMap())
                             .forEach((tp, offsetWithMetadata) -> {
                                 long offset = offsetWithMetadata.offset;
                                 newAssignments.computeIfAbsent(tp.topic(), topic -> new HashMap<>())
                                               .computeIfAbsent(tp.partition(), partition -> {
                                                   Long leo = logEndOffsets.getOrDefault(tp, DEFAULT_OFFSET);
                                                   Long lag = leo == DEFAULT_OFFSET ? 0L : leo - offset;

                                                   return new PartitionAssignment(lag,
                                                                                  offset,
                                                                                  leo,
                                                                                  DEFAULT_CONSUMER_ID,
                                                                                  DEFAULT_CLIENT_ID,
                                                                                  DEFAULT_HOST,
                                                                                  offsetWithMetadata.commitTimestamp);
                                               });
                             });
                consumerGroupInfo.setTopicPartitionAssignments(newAssignments);
            } else {
                partitionAssignments
                        .forEach((topic, offSetMap) ->
                                         offSetMap.forEach((partition, partitionAssignment) -> {
                                             TopicPartition tp = new TopicPartition(topic, partition);
                                             Long leo = logEndOffsets.getOrDefault(tp, DEFAULT_OFFSET);
                                             OffsetWithMetadata metadata =
                                                     commitOffsets.getOrDefault(groupId, Collections.emptyMap())
                                                                  .getOrDefault(tp, OffsetWithMetadata.DEFAULT);
                                             Long offset = metadata.offset;
                                             Long lag = leo == DEFAULT_OFFSET ? 0L : leo - offset;
                                             partitionAssignment.updateOffsets(leo, offset, lag, metadata.commitTimestamp);
                                         })
                        );
            }
            boolean active = isActive(consumerGroupInfo.topicPartitionAssignments());
            consumerGroupInfo.setActive(active);
        }
    }

    private Map<String, ConsumerGroupInfo> buildConsumerGroups(Collection<String> groupNames) {
        Map<String, ConsumerGroupInfo> result = new HashMap<>();

        for (String groupName : groupNames) {
            GroupMetadata groupMetadata = groups.get(groupName);
            GroupSummary summary = groupMetadata.summary();
            String state = summary.state();
            LOG.trace("Consumer group: {}, state: {}", groupName, state);

            if (InvalidGroupStatus.GROUP_DEAD.name.equals(state)) {
                LOG.debug("Ignoring Consumer group: {} with state: {}", groupName, state);
            } else {
                Map<String, Map<Integer, PartitionAssignment>> partitionAssignments =
                        buildPartitionAssignments(groupName, summary);
                boolean active = isActive(partitionAssignments);
                ConsumerGroupInfo consumerGroupInfo = new ConsumerGroupInfo(groupName, state, partitionAssignments, active);
                result.put(groupName, consumerGroupInfo);
            }
        }

        return result;
    }

    private boolean isActive(Map<String, Map<Integer, PartitionAssignment>> partitionAssignments) {
        long currentTimeMs = System.currentTimeMillis();
        for (Map<Integer, PartitionAssignment> assignment : partitionAssignments.values()) {
            for (PartitionAssignment pa : assignment.values()) {
                boolean isActive = (pa.commitTimestamp() + inactiveGroupTimeoutMs) > currentTimeMs;
                if (isActive) {
                    return true;
                }
            }
        }
        return false;
    }

    private void refreshLEOsForAllPartitions() {
        LOG.debug("Refreshing LEOs for all partitions");
        try {
            long startTime = System.currentTimeMillis();
            Set<TopicPartition> partitions = topicPartitionsSupplier.get();
            logEndOffsets = kafkaConsumer.endOffsets(partitions);
            LOG.info("LEOs of all partitions [size: {}] fetched successfully in {} msec", partitions.size(),
                      (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            LOG.warn("Error occurred while getting topics and offsets", e);
        }
    }

    private Map<String, Map<Integer, PartitionAssignment>> buildPartitionAssignments(String groupName,
                                                                                     GroupSummary summary) {
        Map<String, Map<Integer, PartitionAssignment>> partitionAssignments = new HashMap<>();
        Collection<MemberSummary> memberSummaries = JavaConversions.asJavaCollection(summary.members());
        Map<TopicPartition, OffsetWithMetadata> partitionOffsets = commitOffsets.getOrDefault(groupName, Collections.emptyMap());
        Set<TopicPartition> assignedPartitions = new HashSet<>();
        for (MemberSummary member : memberSummaries) {
            String instanceId = member.memberId();
            String clientId = member.clientId();
            String clientHost = member.clientHost();
            List<TopicPartition> partitions =
                    ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(member.assignment())).partitions();
            partitions.forEach(tp -> {
                Long leo = logEndOffsets.getOrDefault(tp, DEFAULT_OFFSET);
                OffsetWithMetadata metadata = partitionOffsets.getOrDefault(tp, OffsetWithMetadata.DEFAULT);
                Long offset = metadata.offset;
                Long lag = leo == DEFAULT_OFFSET ? 0L : leo - offset;
                partitionAssignments.computeIfAbsent(tp.topic(), x -> new HashMap<>())
                                    .put(tp.partition(),
                                         new PartitionAssignment(lag, offset, leo, instanceId, clientId,
                                                                 clientHost, metadata.commitTimestamp));
                assignedPartitions.add(tp);
            });
        }

        if (partitionOffsets != null && !partitionOffsets.isEmpty()) {
            List<TopicPartition> unassignedPartitions =
                    partitionOffsets.keySet()
                                    .stream().filter(k -> !assignedPartitions.contains(k))
                                    .collect(Collectors.toList());
            unassignedPartitions.forEach(tp -> {
                Long leo = logEndOffsets.getOrDefault(tp, DEFAULT_OFFSET);
                OffsetWithMetadata metadata = partitionOffsets.getOrDefault(tp, OffsetWithMetadata.DEFAULT);
                Long offset = metadata.offset;
                Long lag = leo == DEFAULT_OFFSET ? DEFAULT_OFFSET : leo - offset;

                partitionAssignments.computeIfAbsent(tp.topic(), x -> new HashMap<>())
                                    .put(tp.partition(), new PartitionAssignment(lag,
                                                                                 offset,
                                                                                 leo,
                                                                                 DEFAULT_CONSUMER_ID,
                                                                                 DEFAULT_CLIENT_ID,
                                                                                 DEFAULT_HOST,
                                                                                 metadata.commitTimestamp));
            });
        }

        return partitionAssignments;
    }

    private void handleGroupMetadataKey(Set<String> addedGroups,
                                        Set<String> removedGroups,
                                        GroupMetadataKey groupMetadataKey,
                                        ByteBuffer value) {
        String groupId = groupMetadataKey.key();
        GroupMetadata groupMetadata =
                GroupMetadataManager.readGroupMessageValue(groupId, value);
        LOG.debug("Received group metadata [{}], [{}]", groupId, groupMetadata);
        if (groupMetadata == null) {
            LOG.debug("Removing group [{}]", groupId);
            groups.remove(groupId);
            removedGroups.add(groupId);
            addedGroups.remove(groupId);
        } else {
            if (!isInternalGroup(groupId)) {
                groups.put(groupId, groupMetadata);
                addedGroups.add(groupId);
            }
        }
    }

    private boolean isInternalGroup(String groupId) {
        return groupId == null || groupId.isEmpty();
    }

    private void handleOffsetKey(OffsetKey offsetKey, ByteBuffer value) {
        GroupTopicPartition groupTopicPartition = offsetKey.key();
        if (value == null) {
            Map<TopicPartition, OffsetWithMetadata> offsetsMap = commitOffsets.get(groupTopicPartition.group());
            if (offsetsMap != null)
                offsetsMap.remove(groupTopicPartition.topicPartition());
        } else {
            OffsetAndMetadata offsetAndMetadata =
                    GroupMetadataManager.readOffsetMessageValue(value);
            if (offsetAndMetadata != null && offsetAndMetadata.offsetMetadata() != null) {
                OffsetWithMetadata offsetWithMetadata = new OffsetWithMetadata(
                        offsetAndMetadata.offsetMetadata().offset(), offsetAndMetadata.commitTimestamp());
                commitOffsets.computeIfAbsent(groupTopicPartition.group(), x -> new HashMap<>()).
                        put(groupTopicPartition.topicPartition(), offsetWithMetadata);
            }
        }
    }

    Map<String, ConsumerGroupInfo> consumerGroupInfos() {
        return Collections.unmodifiableMap(consumerGroupInfos);
    }

    @Override
    public void close() {
        closed.set(true);
        try {
            scalaAdminClient.close();
        } catch (Exception e) {
        }
        Utils.closeQuietly(kafkaConsumer, "Consumer to fetch consumer group offsets");
    }

    private static class OffsetWithMetadata {
        static final OffsetWithMetadata DEFAULT = new OffsetWithMetadata(DEFAULT_OFFSET, -1L);

        private long offset;
        private long commitTimestamp;

        private OffsetWithMetadata(long offset, long commitTimestamp) {
            this.offset = offset;
            this.commitTimestamp = commitTimestamp;
        }

        @Override
        public String toString() {
            return "OffsetWithMetadata{" +
                    "offset=" + offset +
                    ", commitTimestamp=" + commitTimestamp +
                    '}';
        }
    }
}
