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

import com.hortonworks.registries.schemaregistry.SchemaCompatibility;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.registries.schemaregistry.avro.AvroSchemaProvider;
import com.hortonworks.registries.schemaregistry.client.ISchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.storage.StorageManager;
import com.hortonworks.smm.kafka.services.extension.KafkaAdminServiceTest;
import com.hortonworks.smm.kafka.services.schema.SchemaRegistryService;
import com.hortonworks.smm.kafka.services.schema.dtos.TopicSchemaMapping;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;

import java.io.IOException;
import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;

@KafkaAdminServiceTest()
@DisplayName("Schema registry service tests")
public class SchemaRegistryServiceTest {

    @TestTemplate
    @DisplayName("Test fetching schema versions for a topic")
    public void testGetSchemaVersions(StorageManager storageManager, ISchemaRegistryClient schemaRegistryClient)
            throws Exception {
        String topicName = "topic";
        String keySchemaName = "keySchemaName";
        String valueSchemaName = "valueSchemaName";
        createSchemaVersion(schemaRegistryClient, keySchemaName, "schema/device.avsc");
        createSchemaVersion(schemaRegistryClient, valueSchemaName, "schema/complex_device.avsc");
        SchemaRegistryService schemaRegistryService = new SchemaRegistryService(storageManager, schemaRegistryClient);

        schemaRegistryService
                .registerTopicSchemaMapping(topicName, new TopicSchemaMapping(keySchemaName, valueSchemaName));
        Collection<SchemaVersionInfo> expectedKeySchemaVersions = schemaRegistryClient.getAllVersions(keySchemaName);
        Collection<SchemaVersionInfo> expectedValueSchemaVersions =
                schemaRegistryClient.getAllVersions(valueSchemaName);

        assertEquals(expectedKeySchemaVersions, schemaRegistryService.getKeySchemaVersionInfos(topicName));
        assertEquals(expectedValueSchemaVersions, schemaRegistryService.getValueSchemaVersionInfos(topicName));
    }

    @TestTemplate
    @DisplayName("Test registering invalid schema version")
    public void testRegisterInvalidSchemaVersion(SchemaRegistryService schemaRegistryService) throws Exception {
        String topicName = "topic";

        Assertions.assertThrows(NullPointerException.class, () ->
                schemaRegistryService.registerTopicSchemaMapping(topicName, new TopicSchemaMapping(null, null)));
    }

    @TestTemplate
    @DisplayName("Test fetching schema names registered to a topic")
    public void testGetTopicSchemaMapping(SchemaRegistryService schemaRegistryService) throws Exception {
        String topicName = "topic";
        String keySchemaName = "keySchemaName";
        String valueSchemaName = "valueSchemaName";

        TopicSchemaMapping expectedTopicSchemaMapping = new TopicSchemaMapping(keySchemaName, valueSchemaName);
        schemaRegistryService.registerTopicSchemaMapping(topicName, expectedTopicSchemaMapping);

        assertEquals(expectedTopicSchemaMapping, schemaRegistryService.getTopicSchemaMeta(topicName).get());
    }

    private void createSchemaVersion(ISchemaRegistryClient schemaRegistryClient, String schemaName,
                                     String schemaTextFilePath) throws IOException, SchemaNotFoundException,
                                                                       InvalidSchemaException,
                                                                       IncompatibleSchemaException {
        SchemaMetadata schemaMetadata = buildSchemaMetadata(schemaName, SchemaCompatibility.NONE);
        String schemaText =
                IOUtils.toString(this.getClass().getClassLoader().getResourceAsStream(schemaTextFilePath), "UTF-8");
        SchemaVersion schemaVersion = new SchemaVersion(schemaText, schemaName);
        schemaRegistryClient.addSchemaMetadata(schemaMetadata);
        schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaVersion);
    }

    private SchemaMetadata buildSchemaMetadata(String schemaDesc, SchemaCompatibility compatibility) {
        return new SchemaMetadata.Builder(schemaDesc)
                .type(AvroSchemaProvider.TYPE)
                .schemaGroup(schemaDesc + "-group")
                .description(schemaDesc)
                .compatibility(compatibility)
                .build();
    }

}
