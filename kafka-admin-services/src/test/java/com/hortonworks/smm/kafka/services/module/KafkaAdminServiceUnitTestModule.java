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

package com.hortonworks.smm.kafka.services.module;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.MockSchemaRegistryClient;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.impl.memory.InMemoryStorageManager;
import com.hortonworks.smm.kafka.common.config.KafkaAdminClientConfig;
import com.hortonworks.smm.kafka.common.config.KafkaConsumerConfig;
import com.hortonworks.smm.kafka.common.config.KafkaManagementConfig;
import com.hortonworks.smm.kafka.common.config.KafkaMetricsConfig;
import com.hortonworks.smm.kafka.common.extension.KafkaAdminTestConstants;
import com.hortonworks.smm.kafka.common.extension.KafkaAdminTestEnvironment;
import com.hortonworks.smm.kafka.common.module.StartableModule;
import com.hortonworks.smm.kafka.services.EmbeddedKafkaClusterWrapper;
import com.hortonworks.smm.kafka.services.config.KafkaProducerConfig;
import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.metric.MetricsFetcher;
import com.hortonworks.smm.kafka.services.metric.MockMetricsFetcher;
import com.hortonworks.smm.kafka.services.schema.SchemaRegistryService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public class KafkaAdminServiceUnitTestModule extends AbstractModule implements StartableModule {

    private EmbeddedKafkaClusterWrapper kafkaCluster = null;

    @Override
    public void start(Map<String, Object> props) {
        int numOfBrokerNodes = (int) props.get(KafkaAdminTestConstants.NUM_BROKER_NODES);
        if (numOfBrokerNodes != 0) {
            kafkaCluster = new EmbeddedKafkaClusterWrapper(numOfBrokerNodes);
            kafkaCluster.startUp();
        }
    }

    @Override
    public void stop(Map<String, Object> props) {
        if (kafkaCluster != null) {
            kafkaCluster.tearDown();
        }
    }

    @Override
    protected void configure() {
        bind(ISchemaRegistryClient.class).to(MockSchemaRegistryClient.class);
    }

    @Provides
    @Singleton
    public KafkaProducerConfig provideKafkaProducerProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.RETRIES_CONFIG, 5);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducerConfig(properties);
    }

    @Provides
    @Singleton
    public KafkaAdminClientConfig providesKafkaAdminClientConfig() {
        Map<String,Object> properties = new HashMap<>();
        properties.put("properties", Collections.EMPTY_MAP);
        return new KafkaAdminClientConfig(kafkaCluster.bootstrapServers(), properties);
    }

    @Provides
    @Singleton
    public KafkaConsumerConfig provideKafkaConsumerConfig() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(KafkaConsumerConfig.POLL_TIMEOUT_MS_PROPERTY_NAME, "30000");
        properties.put("properties", Collections.EMPTY_MAP);
        return new KafkaConsumerConfig(kafkaCluster.bootstrapServers(), null, properties);
    }

    @Provides
    @Singleton
    public KafkaManagementConfig providesKafkaManagementConfig() {
        return new KafkaManagementConfig(3000L);
    }

    @Provides
    public KafkaMetricsConfig provideMetricsConfig() {
        return new KafkaMetricsConfig(MockMetricsFetcher.class.getName(), 30000L, 30000L, 1800_000L, 1800_000L, 20, Collections.EMPTY_MAP);
    }

    @Provides
    public AdminClient provideAdminClient(KafkaAdminClientConfig kafkaAdminClientConfig) {
        return AdminClient.create(kafkaAdminClientConfig.getConfig());
    }

    @Provides
    public SchemaRegistryService provideSchemaRegistryService(StorageManager storageManager, ISchemaRegistryClient schemaRegistryClient) {
        return new SchemaRegistryService(storageManager, schemaRegistryClient);
    }

    @Provides
    public MockSchemaRegistryClient provideSchemaRegistryClient() {
        return new MockSchemaRegistryClient();
    }

    @Provides
    @Singleton
    public StorageManager provideStorageManager() {
        return new InMemoryStorageManager();
    }

    @Provides
    public KafkaAdminTestEnvironment provideKafkaAdminTestEnvironment() {
        return KafkaAdminTestEnvironment.UNIT;
    }

    @Provides
    @Singleton
    public MetricsFetcher provideMetricsFetcher(TopicManagementService topicMgmtService) {
        return new MockMetricsFetcher(topicMgmtService);
    }
}