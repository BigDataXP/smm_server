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

package com.hortonworks.smm.kafka.services.message;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.smm.kafka.common.config.KafkaConsumerConfig;
import com.hortonworks.smm.kafka.services.Service;
import com.hortonworks.smm.kafka.services.message.dtos.DeserializerInfo;
import com.hortonworks.smm.kafka.services.message.dtos.PartitionOffsetInfo;
import com.hortonworks.smm.kafka.services.message.dtos.SerdesMappings;
import com.hortonworks.smm.kafka.services.message.dtos.TopicContent;
import com.hortonworks.smm.kafka.services.message.dtos.TopicOffsetInfo;
import com.hortonworks.smm.kafka.services.message.dtos.TopicRecord;
import com.hortonworks.smm.kafka.services.message.util.TopicMessageServiceUtil;
import com.hortonworks.smm.kafka.services.schema.SchemaRegistryService;
import com.hortonworks.smm.kafka.services.schema.dtos.TopicSchemaMapping;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Singleton
public class TopicMessageService implements Service {
    public static final String POLLING_TIMEOUT_INTERVAL_PROPERTY = "poll.timeout.ms";

    private static final String DEFAULT_DESERIALIZER_CLASS = StringDeserializer.class.getName();
    private static final String CONSUMER_ID_FORMAT = "_streams_messaging_manager_consumer_%s";
    private static final Logger LOG = LoggerFactory.getLogger(TopicMessageService.class);
    public static final long DEFAULT_RESPONSE_TIME_OUT_IN_MS = 30 * 1000L;

    private static SerdesMappings SERDES_MAPPING;
    private KafkaConsumerConfig config;
    private SchemaRegistryService schemaRegistryService;
    private long pollingIntervalInMs;

    static {
        List<DeserializerInfo> deserializerInfos = Lists.newArrayList(
                new DeserializerInfo("String",
                                     StringDeserializer.class.getName(),
                                    "A deserializer for String type"),
                new DeserializerInfo("Short",
                                     ShortDeserializer.class.getName(),
                                    "A deserializer for Short type"),
                new DeserializerInfo("Integer",
                                     IntegerDeserializer.class.getName(),
                                    "A deserializer for Integer type"),
                new DeserializerInfo("Long",
                                     LongDeserializer.class.getName(),
                                    "A deserializer for Long type"),
                new DeserializerInfo("Float",
                                     FloatDeserializer.class.getName(),
                                    "A deserializer for Float type"),
                new DeserializerInfo("Double",
                                     DoubleDeserializer.class.getName(),
                                    "A deserializer for Double type"),
                new DeserializerInfo("Bytes",
                                     BytesDeserializer.class.getName(),
                                    "A deserializer for Bytes type"),
                new DeserializerInfo("ByteArray",
                                     ByteArrayDeserializer.class.getName(),
                                    "A deserializer for ByteArray type"),
                new DeserializerInfo("ByteBuffer",
                                     ByteBufferDeserializer.class.getName(),
                                    "A deserializer for ByteBuffer type"),
                new DeserializerInfo("Avro",
                                     KafkaAvroDeserializer.class.getName(),
                                    "A deserializer for KafkaAvro type"));

        SERDES_MAPPING = new SerdesMappings(deserializerInfos);
    }

    @Inject
    public TopicMessageService(KafkaConsumerConfig config, SchemaRegistryService schemaRegistryService) {
        this.config = config;
        this.schemaRegistryService = schemaRegistryService;
        pollingIntervalInMs = config.getPollTimeOutMs();
    }

    public TopicOffsetInfo getOffset(String topicName) {
        Map<String, Object> consumerConfig =
                generateConsumerConfig(config, DEFAULT_DESERIALIZER_CLASS, DEFAULT_DESERIALIZER_CLASS);

        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerConfig)) {
            List<TopicPartition> topicPartitions = new ArrayList<>();
            for (PartitionInfo partitionInfo : consumer.partitionsFor(topicName)) {
                topicPartitions.add(new TopicPartition(topicName, partitionInfo.partition()));
            }

            Map<TopicPartition, Long> startOffset = consumer.beginningOffsets(topicPartitions);
            Map<TopicPartition, Long> endOffset = consumer.endOffsets(topicPartitions);

            LOG.info("startOffset[{}], endOffset [{}]", startOffset, endOffset);

            List<PartitionOffsetInfo> partitionOffsetInfos = new ArrayList<>();

            for (Map.Entry<TopicPartition, Long> startOffsetEntry : startOffset.entrySet()) {
                partitionOffsetInfos.add(new PartitionOffsetInfo(startOffsetEntry.getKey().partition(),
                                                                 startOffsetEntry.getValue(),
                                                                 endOffset.get(startOffsetEntry.getKey())));
            }
            return new TopicOffsetInfo(partitionOffsetInfos);
        }
    }

    public TopicContent getTopicContent(String topicName, int partitionId, long startOffset, long endOffset,
                                        String keyDeserializerClass, String valueDeserializerClass) {
        return getTopicContent(topicName, partitionId, startOffset, endOffset, keyDeserializerClass,
                               valueDeserializerClass, DEFAULT_RESPONSE_TIME_OUT_IN_MS);
    }

    public TopicContent getTopicContent(String topicName, int partitionId, long startOffset, long endOffset,
                                        String keyDeserializerClass, String valueDeserializerClass,
                                        long responseTimeOutInMs) {
        Objects.requireNonNull(topicName, "topicName can not be null");
        Validate.inclusiveBetween(0, Long.MAX_VALUE, startOffset);
        Validate.inclusiveBetween(0, Long.MAX_VALUE, endOffset);

        Map<String, Object> consumerConfig =
                generateConsumerConfig(config, keyDeserializerClass, valueDeserializerClass);

        try (KafkaConsumer<Object, Object> consumer = new KafkaConsumer<>(consumerConfig)) {
            checkTopicPartition(topicName, partitionId, consumer);

            if (startOffset == 0 && endOffset == 0) {
                return buildTopicContent(schemaRegistryService.getTopicSchemaMeta(topicName), Collections.emptyMap());
            }

            Validate.isTrue(startOffset < endOffset,
                            "startOffset %d should be less than endOffset %d", startOffset, endOffset);

            TopicPartition topicPartition = new TopicPartition(topicName, partitionId);
            consumer.assign(Collections.singletonList(topicPartition));

            Long logEndOffset = consumer.endOffsets(Collections.singletonList(topicPartition)).get(topicPartition);
            LOG.info("logEndOffset [{}], endOffSet[{}], startOffset [{}]", logEndOffset, endOffset, startOffset);

            Validate.validState(startOffset < logEndOffset,
                                "Given startOffset %d is more than log end offset %d",
                                startOffset, logEndOffset);
            Validate.validState(endOffset <= logEndOffset,
                                "Given endOffset %d is more than log end offset %d",
                                endOffset, logEndOffset);

            consumer.seek(topicPartition, startOffset);
            Long currentOffset = startOffset - 1;
            ConsumerRecords<Object, Object> consumerRecords = null;
            Map<Long, TopicRecord> offsetWithRecord = new HashMap<>();
            long waitTill = System.currentTimeMillis() + responseTimeOutInMs;
            long consumableEndOffset = endOffset - 1;
            do {
                consumerRecords = consumer.poll(pollingIntervalInMs);
                for (ConsumerRecord<Object, Object> record : consumerRecords) {
                    offsetWithRecord.put(record.offset(),
                                         new TopicRecord(TopicMessageServiceUtil.serialize(record.key()),
                                                         TopicMessageServiceUtil.serialize(record.value()),
                                                         record.timestampType(), record.timestamp()));
                    currentOffset++;
                    if (currentOffset == consumableEndOffset)
                        break;
                }
                LOG.info("################### currentOffset [{}], endOffSet[{}]", currentOffset, endOffset,
                         startOffset);
            } while (currentOffset < consumableEndOffset && waitTill - System.currentTimeMillis() > 0);

            return buildTopicContent(schemaRegistryService.getTopicSchemaMeta(topicName), offsetWithRecord);
        }
    }

    public SerdesMappings getSerdesMapping() {
        return SERDES_MAPPING;
    }

    private void checkTopicPartition(String topicName, int partitionId, KafkaConsumer<Object, Object> consumer) {
        for (PartitionInfo partitionInfo : consumer.partitionsFor(topicName)) {
            if (partitionInfo.partition() == partitionId) {
                return;
            }
        }

        throw new IllegalArgumentException(
                "Given topic [" + topicName + "] partition [" + partitionId + "] does not exist");
    }

    private TopicContent buildTopicContent(Optional<TopicSchemaMapping> topicSchemaMapping,
                                           Map<Long, TopicRecord> offsetWithRecord) {
        return topicSchemaMapping
                .map(x -> new TopicContent(x.getKeySchemaName(), x.getValueSchemaName(), offsetWithRecord))
                .orElseGet(() -> new TopicContent(offsetWithRecord));
    }

    private Map<String, Object> generateConsumerConfig(KafkaConsumerConfig config, String keyDeserializerClass,
                                                       String valueDeserializerClass) {
        Map<String, Object> consumerConfig = new HashMap<>(config.getConfig());
        consumerConfig.put(ConsumerConfig.CLIENT_ID_CONFIG, String.format(CONSUMER_ID_FORMAT, UUID.randomUUID()));
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializerClass);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializerClass);
        return consumerConfig;
    }


    @Override
    public void close() throws Exception {
    }
}
