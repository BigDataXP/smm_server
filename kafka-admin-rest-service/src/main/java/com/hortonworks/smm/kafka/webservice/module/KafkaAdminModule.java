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

package com.hortonworks.smm.kafka.webservice.module;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.hortonworks.registries.storage.NOOPTransactionManager;
import com.hortonworks.registries.storage.TransactionManager;
import com.hortonworks.smm.kafka.common.config.KafkaAdminClientConfig;
import com.hortonworks.smm.kafka.common.config.KafkaConsumerConfig;
import com.hortonworks.smm.kafka.common.config.KafkaManagementConfig;
import com.hortonworks.smm.kafka.common.config.KafkaMetricsConfig;
import com.hortonworks.smm.kafka.common.config.SchemaRegistryClientConfig;
import com.hortonworks.smm.kafka.services.management.TopicManagementService;
import com.hortonworks.smm.kafka.services.metric.MetricsFetcher;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.webservice.config.SMMConfig;
import com.hortonworks.registries.common.util.ReflectionHelper;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.storage.Storable;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.registries.storage.StorageProviderConfiguration;
import com.hortonworks.registries.storage.annotation.StorableEntity;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.config.ConfigException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

public class KafkaAdminModule extends AbstractModule {

    private SMMConfig smmConfig;
    private SMMAuthorizer authorizer;

    public KafkaAdminModule(SMMConfig smmConfig, SMMAuthorizer authorizer) {
        this.smmConfig = smmConfig;
        this.authorizer = authorizer;
    }

    @Override
    protected void configure() {
        bind(ISchemaRegistryClient.class).to(SchemaRegistryClient.class);
    }


    // Configurations

    @Provides
    @Singleton
    public KafkaAdminClientConfig providesKafkaAdminClientConfig() {
        return new KafkaAdminClientConfig(smmConfig.getKafkaBootstrapServers(), smmConfig.getKafkaAdminClientConfig());
    }

    @Provides
    @Singleton
    public KafkaConsumerConfig providesKafkaConsumerConfig() {
        return new KafkaConsumerConfig(smmConfig.getKafkaBootstrapServers(), smmConfig.getSchemaRegistryUrl(),
                smmConfig.getKafkaConsumerClientConfig());
    }

    @Provides
    @Singleton
    public KafkaMetricsConfig providesKafkaMetricsConfig() {
        return smmConfig.getKafkaMetricsConfig();
    }

    @Provides
    @Singleton
    public SchemaRegistryClientConfig providesSchemaRegistryClientConfig() {
        return new SchemaRegistryClientConfig(smmConfig.getSchemaRegistryUrl(), smmConfig.getSchemaRegistryClientConfig());
    }

    @Provides
    @Singleton
    public KafkaManagementConfig providesKafkaManagementConfig() {
        return smmConfig.getKafkaManagementConfig();
    }

    // Management Service

    @Provides
    public AdminClient provideAdminClient(KafkaAdminClientConfig kafkaAdminClientConfig) {
        return AdminClient.create(kafkaAdminClientConfig.getConfig());
    }


    @Provides
    public kafka.admin.AdminClient providesScalaBasedAdminClient(KafkaAdminClientConfig kafkaAdminClientConfig) {
        Properties properties = new Properties();
        properties.putAll(kafkaAdminClientConfig.getConfig());
        return kafka.admin.AdminClient.create(properties);
    }

    // Metrics Service

    @Provides
    @Singleton
    public MetricsFetcher providesMetricsFetcher(KafkaMetricsConfig metricsConfig, TopicManagementService topicMgmtService) {
        if (metricsConfig == null) {
            return MetricsFetcher.NO_OP;
        }

        String metricsClassName = metricsConfig.getMetricsFetcherClass();
        try {
            Constructor<?> constructor = Class.forName(metricsClassName).getConstructor(TopicManagementService.class);
            MetricsFetcher metricsFetcher = (MetricsFetcher) constructor.newInstance(topicMgmtService);
            metricsFetcher.configure(metricsConfig.getConfig());
            return metricsFetcher;
        } catch (IllegalAccessException | InstantiationException | NoSuchMethodException | ClassNotFoundException |
                InvocationTargetException | ConfigException e) {
            throw new RuntimeException("Can't initialize metrics fetcher class: " + metricsClassName, e);
        }
    }

    // Schema Service

    @Provides
    public SchemaRegistryClient provideSchemaRegistryClient(SchemaRegistryClientConfig schemaRegistryClientConfig) {
        String schemaRegistryURL = (String) schemaRegistryClientConfig.getConfig().get(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name());
        if (schemaRegistryURL == null || schemaRegistryURL.isEmpty()) {
            return null;
        } else {
            return new SchemaRegistryClient(schemaRegistryClientConfig.getConfig());
        }
    }

    @Provides
    @Singleton
    public StorageManager provideStorageManager() {
        StorageProviderConfiguration storageProviderConfiguration = smmConfig.getStorageProviderConfig();
        final String providerClass = storageProviderConfiguration.getProviderClass();
        StorageManager storageManager;
        try {
            storageManager = (StorageManager) Class.forName(providerClass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        storageManager.init(storageProviderConfiguration.getProperties());
        storageManager.registerStorables(getStorableEntities());
        return storageManager;
    }

    private static Collection<Class<? extends Storable>> getStorableEntities() {
        Set<Class<? extends Storable>> entities = new HashSet<>();
        ReflectionHelper.getAnnotatedClasses("com.hortonworks.smm.kafka.services.core", StorableEntity.class)
                .forEach((clazz) -> {
                    if (Storable.class.isAssignableFrom(clazz)) {
                        entities.add((Class<? extends Storable>) clazz);
                    }

                });
        return entities;
    }

    @Provides
    @Singleton
    public TransactionManager providesTransactionManager(StorageManager storageManager) {
        if (storageManager instanceof TransactionManager) {
            return (TransactionManager) storageManager;
        } else {
            return new NOOPTransactionManager();
        }
    }

    // Authorizer

    @Provides
    public SMMAuthorizer providesAuthorizer() {
        return authorizer;
    }

}
