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

package com.hortonworks.smm.kafka.services;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.smm.kafka.common.config.KafkaConsumerConfig;
import com.hortonworks.smm.kafka.common.utils.AdminClientUtil;
import com.hortonworks.smm.kafka.services.config.KafkaProducerConfig;
import com.hortonworks.smm.kafka.services.extension.KafkaAdminServiceTest;
import com.hortonworks.smm.kafka.services.helper.KafkaProducerPayloadGenerator;
import com.hortonworks.smm.kafka.services.message.TopicMessageService;
import com.hortonworks.smm.kafka.services.message.dtos.PartitionOffsetInfo;
import com.hortonworks.smm.kafka.services.message.dtos.TopicContent;
import com.hortonworks.smm.kafka.services.message.dtos.TopicOffsetInfo;
import com.hortonworks.smm.kafka.services.message.dtos.TopicRecord;
import com.hortonworks.smm.kafka.services.message.util.TopicMessageServiceUtil;
import com.hortonworks.smm.kafka.services.schema.SchemaRegistryService;
import com.hortonworks.smm.kafka.services.schema.dtos.TopicSchemaMapping;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaAdminServiceTest(numBrokerNodes = 3)
@DisplayName("Topic message service tests")
public class TopicMessageServiceTest {

    private static final int TIME_OUT = 60 * 1000;

    @TestTemplate
    @DisplayName("Test get topic offsets")
    public void testGetOffset(AdminClient adminClient,
                              KafkaProducerConfig producerConfig,
                              KafkaConsumerConfig consumerConfig,
                              SchemaRegistryService schemaRegistryService, TestInfo testInfo)
            throws Exception {
        String topicName = testInfo.getTestMethod().get().getName();
        int noPartitions = 2;
        Short noReplicas = 2;
        List<PartitionOffsetInfo> partitionOffsetInfos = new ArrayList<>();
        String keySerializer = DoubleSerializer.class.getName();
        String valueSerializer = DoubleSerializer.class.getName();

        AdminClientUtil.createTopic(adminClient, new NewTopic(topicName, noPartitions, noReplicas), TIME_OUT);

        try (KafkaProducer<Double, Double> kafkaProducer = createKafkaProducer(producerConfig, keySerializer,
                                                                               valueSerializer)) {
            for (int partitionId = 0; partitionId < noPartitions; partitionId++) {
                kafkaProducer
                        .send(new ProducerRecord<Double, Double>(topicName, partitionId, Math.random(), Math.random()))
                        .get();
                partitionOffsetInfos.add(new PartitionOffsetInfo(partitionId, 0, 1));
            }
        }

        TopicOffsetInfo actualOffsetInfo =
                topicMessageService(consumerConfig, schemaRegistryService).getOffset(topicName);
        TopicOffsetInfo expectedOffsetInfo = new TopicOffsetInfo(partitionOffsetInfos);

        assertEquals(expectedOffsetInfo, actualOffsetInfo);

        AdminClientUtil.deleteTopic(adminClient, topicName, TIME_OUT);
    }

    @TestTemplate
    @DisplayName("Test get offset for non-existent topic")
    public void testGetOffsetForNonexistentTopic(
            KafkaConsumerConfig consumerConfig,
            SchemaRegistryService schemaRegistryService,
            TestInfo testInfo) throws Exception {
        TopicOffsetInfo actualOffsetInfo =
                topicMessageService(consumerConfig, schemaRegistryService)
                        .getOffset(testInfo.getTestMethod().get().getName());
        TopicOffsetInfo expectedOffsetInfo =
                new TopicOffsetInfo(Collections.singletonList(new PartitionOffsetInfo(0, 0, 0)));

        assertEquals(expectedOffsetInfo, actualOffsetInfo);
    }


    @TestTemplate
    @DisplayName("Test topic content")
    public void testTopicContent(AdminClient adminClient,
                                 KafkaProducerConfig producerConfig,
                                 KafkaConsumerConfig consumerConfig,
                                 SchemaRegistryService schemaRegistryService, TestInfo testInfo) throws Exception {
        int noPartitions = 2;
        Short noReplicas = 2;
        for (KafkaProducerPayloadGenerator.Type type : KafkaProducerPayloadGenerator.Type.values()) {
            String topicName = testInfo.getTestMethod().get().getName() + "." + type.name();
            String keySchema = topicName + "-keySchema";
            String valueSchema = topicName + "-valueSchema";
            List<TopicContent> topicContents = new ArrayList<>();
            KafkaProducerPayloadGenerator generator = new KafkaProducerPayloadGenerator(type);

            AdminClientUtil.createTopic(adminClient, new NewTopic(topicName, noPartitions, noReplicas), TIME_OUT);

            try (KafkaProducer<Object, Object> kafkaProducer = createKafkaProducer(producerConfig,
                                                                                   generator.getSerializerClass(),
                                                                                   generator.getSerializerClass())) {
                for (int partitionId = 0; partitionId < noPartitions; partitionId++) {
                    Map<Long, TopicRecord> topicRecordMap = new HashMap<>();
                    Object key = generator.getNext();
                    Object value = generator.getNext();
                    RecordMetadata recordMetadata =
                            kafkaProducer.send(new ProducerRecord<>(topicName, partitionId, key, value)).get();
                    topicRecordMap.put(recordMetadata.offset(),
                                       new TopicRecord(
                                               TopicMessageServiceUtil.serialize(key),
                                               TopicMessageServiceUtil.serialize(value),
                                               TimestampType.CREATE_TIME, recordMetadata.timestamp()));

                    topicContents.add(new TopicContent(keySchema, valueSchema, topicRecordMap));
                }
            }

            schemaRegistryService.registerTopicSchemaMapping(topicName, new TopicSchemaMapping(keySchema, valueSchema));

            for (int partition = 0; partition < noPartitions; partition++) {
                assertEquals(topicContents.get(partition),
                             topicMessageService(consumerConfig, schemaRegistryService)
                                     .getTopicContent(topicName, partition, 0l, 1l, generator.getDeserializerClass(),
                                                      generator.getDeserializerClass()));
            }

            AdminClientUtil.deleteTopic(adminClient, topicName, TIME_OUT);
        }
    }

    @TestTemplate
    @DisplayName("Test get topic content for non-existent topic")
    public void testTopicContentForNonexistentTopic(KafkaConsumerConfig consumerConfig,
                                                    SchemaRegistryService schemaRegistryService,
                                                    TestInfo testInfo) {
        String topicName = testInfo.getTestMethod().get().getName();
        String keySchema = topicName + "-keySchema";
        String valueSchema = topicName + "-valueSchema";
        TopicContent topicContent = new TopicContent(keySchema, valueSchema, new HashMap<>());
        String keyDeserializer = LongDeserializer.class.getName();
        String valueDeserializer = LongDeserializer.class.getName();

        schemaRegistryService.registerTopicSchemaMapping(topicName, new TopicSchemaMapping(keySchema, valueSchema));

        assertEquals(topicContent,
                     topicMessageService(consumerConfig, schemaRegistryService)
                             .getTopicContent(topicName, 0, 0l, 0l, keyDeserializer,
                                              valueDeserializer, 0l));
    }

    @TestTemplate
    @DisplayName("Test get topic content for non-existent partition")
    public void testTopicContentForNonexistentPartition(AdminClient adminClient,
                                                        KafkaConsumerConfig consumerConfig,
                                                        SchemaRegistryService schemaRegistryService,
                                                        TestInfo testInfo) {
        String topicName = testInfo.getTestMethod().get().getName();
        int noPartitions = 2;
        Short noReplicas = 2;
        String keySchema = topicName + "-keySchema";
        String valueSchema = topicName + "-valueSchema";
        String keyDeserializer = LongDeserializer.class.getName();
        String valueDeserializer = LongDeserializer.class.getName();
        TopicContent topicContent = new TopicContent(keySchema, valueSchema, new HashMap<>());

        AdminClientUtil.createTopic(adminClient, new NewTopic(topicName, noPartitions, noReplicas), TIME_OUT);

        schemaRegistryService.registerTopicSchemaMapping(topicName, new TopicSchemaMapping(keySchema, valueSchema));

        Assertions.assertThrows(IllegalArgumentException.class, () -> topicMessageService(consumerConfig, schemaRegistryService).getTopicContent(
                topicName, 20989, 0l, 0l, keyDeserializer, valueDeserializer));

        AdminClientUtil.deleteTopic(adminClient, topicName, TIME_OUT);
    }

    private TopicMessageService topicMessageService(KafkaConsumerConfig consumerConfig,
                                                    SchemaRegistryService schemaRegistryService) {
        Map<String,Object> config = new HashMap<>();
        config.putAll(consumerConfig.getConfig());
        config.put(TopicMessageService.POLLING_TIMEOUT_INTERVAL_PROPERTY, 500);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        String bootstrapServerString = (String) consumerConfig.getConfig().get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
        String schemaRegistryURL = (String) consumerConfig.getConfig().get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name());
        KafkaConsumerConfig updateConsumerConfig = new KafkaConsumerConfig(bootstrapServerString, schemaRegistryURL, config);
        return new TopicMessageService(updateConsumerConfig, schemaRegistryService);
    }

    private <T> KafkaProducer<T, T> createKafkaProducer(KafkaProducerConfig producerConfig,
                                                        String keySerializer,
                                                        String valueSerializer) {
        Map<String,Object> config = new HashMap<>();
        config.putAll(producerConfig.getProperties());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        return new KafkaProducer<>(config);
    }
}
