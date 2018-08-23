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
package com.hortonworks.smm.kafka.services.schema;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.smm.kafka.services.Service;
import com.hortonworks.smm.kafka.services.common.exceptions.EntityNotFoundServiceException;
import com.hortonworks.smm.kafka.services.common.exceptions.SchemaRegistryClientNotInitializedException;
import com.hortonworks.smm.kafka.services.core.TopicSchemaMappingStorable;
import com.hortonworks.smm.kafka.services.schema.dtos.TopicSchemaMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 *
 */

@Singleton
public class SchemaRegistryService implements Service {

    private StorageManager storageManager;
    private ISchemaRegistryClient schemaRegistryClient;
    public static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryService.class);

    @Inject
    public SchemaRegistryService(StorageManager storageManager, @Nullable ISchemaRegistryClient schemaRegistryClient) {
        this.storageManager = storageManager;
        this.schemaRegistryClient = schemaRegistryClient;

        if (schemaRegistryClient == null) {
            LOG.warn("Schema registry client is initialized as null as schemaRegistryURL is either null or empty string");
        }
    }

    public Collection<SchemaVersionInfo> getKeySchemaVersionInfos(String topic)
            throws SchemaNotFoundException, EntityNotFoundServiceException, SchemaRegistryClientNotInitializedException {
        if (schemaRegistryClient == null)
            throw new SchemaRegistryClientNotInitializedException();

        TopicSchemaMappingStorable topicSchemaMappingStorable = storageManager.get(new TopicSchemaMappingStorable(topic)
                                                                                           .getStorableKey());
        if (topicSchemaMappingStorable != null) {
            if (topicSchemaMappingStorable.getKeySchemaName() == null) {
                return Collections.emptyList();
            } else {
                return schemaRegistryClient.getAllVersions(topicSchemaMappingStorable.getKeySchemaName());
            }
        } else {
            throw new EntityNotFoundServiceException(topic);
        }
    }

    public Collection<SchemaVersionInfo> getValueSchemaVersionInfos(String topic)
            throws SchemaNotFoundException, EntityNotFoundServiceException, SchemaRegistryClientNotInitializedException {
        if (schemaRegistryClient == null)
            throw new SchemaRegistryClientNotInitializedException();

        TopicSchemaMappingStorable topicSchemaMappingStorable =
                storageManager.get(new TopicSchemaMappingStorable(topic).getStorableKey());

        if (topicSchemaMappingStorable != null) {
            return schemaRegistryClient.getAllVersions(topicSchemaMappingStorable.getValueSchemaName());
        } else {
            throw new EntityNotFoundServiceException(topic);
        }
    }

    public Long registerTopicSchemaMapping(String topicName, TopicSchemaMapping schemaMetaForTopic) {
        TopicSchemaMappingStorable topicSchemaMappingStorable =
                new TopicSchemaMappingStorable(topicName,
                                               schemaMetaForTopic.getKeySchemaName(),
                                               schemaMetaForTopic.getValueSchemaName());
        topicSchemaMappingStorable.setId(storageManager.nextId(TopicSchemaMappingStorable.NAME_SPACE));
        topicSchemaMappingStorable.setTimestamp(System.currentTimeMillis());
        storageManager.addOrUpdate(topicSchemaMappingStorable);

        return storageManager.get(topicSchemaMappingStorable.getStorableKey()).getId();
    }

    public Optional<TopicSchemaMapping> getTopicSchemaMeta(String topicName) {
        TopicSchemaMappingStorable topicSchemaMappingStorable =
                storageManager.get(new TopicSchemaMappingStorable(topicName).getStorableKey());

        return topicSchemaMappingStorable != null
               ? Optional.of(new TopicSchemaMapping(topicSchemaMappingStorable.getKeySchemaName(),
                                                    topicSchemaMappingStorable.getValueSchemaName()))
               : Optional.empty();
    }

    @Override
    public void close() throws Exception {
        if (schemaRegistryClient != null) {
            schemaRegistryClient.close();
        }
        if (storageManager != null) {
            storageManager.cleanup();
        }
    }
}
