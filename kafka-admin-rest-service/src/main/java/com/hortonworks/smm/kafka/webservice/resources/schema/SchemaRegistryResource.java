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

package com.hortonworks.smm.kafka.webservice.resources.schema;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.hortonworks.registries.common.exception.service.exception.request.EntityNotFoundException;
import com.hortonworks.registries.common.exception.service.exception.server.UnhandledServerException;
import com.hortonworks.registries.common.transaction.UnitOfWork;
import com.hortonworks.registries.schemaregistry.SchemaVersionInfo;
import com.hortonworks.smm.kafka.services.common.exceptions.EntityNotFoundServiceException;
import com.hortonworks.smm.kafka.services.schema.SchemaRegistryService;
import com.hortonworks.smm.kafka.services.schema.dtos.TopicSchemaMapping;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.Objects;

@Path("/api/v1/admin/topics")
@Api(value = "/v1", description = "Endpoint for Schema Registry service")
@Produces(MediaType.APPLICATION_JSON)
public class SchemaRegistryResource {

    private static final String DESCRIPTION = "Schema registry operations";

    private SchemaRegistryService schemaRegistryService;

    @Inject
    public SchemaRegistryResource(SchemaRegistryService schemaRegistryService) {
        Objects.requireNonNull(schemaRegistryService, "schemaRegistryService must not be null");

        this.schemaRegistryService = schemaRegistryService;
    }

    @GET
    @Path("/{topicName}/keySchema/versions")
    @ApiOperation(value = "Get schema versions for the given kafka topic name",
            response = SchemaVersionInfo.class, responseContainer = "List",
            tags = DESCRIPTION)
    @Timed
    @UnitOfWork
    public Collection<SchemaVersionInfo> getKeySchemaVersionInfos(@PathParam("topicName") String topicName) {
        try {
            return schemaRegistryService.getKeySchemaVersionInfos(topicName);
        } catch (EntityNotFoundServiceException e) {
            throw EntityNotFoundException.byId(e.getId());
        } catch (Exception e) {
            throw new UnhandledServerException(
                    String.format("Failed to fetch schema versions for topic : '%s'", topicName), e);
        }
    }

    @GET
    @Path("/{topicName}/valueSchema/versions")
    @ApiOperation(value = "Get schema versions for the given kafka topic name",
            response = SchemaVersionInfo.class, responseContainer = "List",
            tags = DESCRIPTION)
    @Timed
    @UnitOfWork
    public Collection<SchemaVersionInfo> getValueSchemaVersionInfos(@PathParam("topicName") String topicName) {
        try {
            return schemaRegistryService.getValueSchemaVersionInfos(topicName);
        } catch (EntityNotFoundServiceException e) {
            throw EntityNotFoundException.byId(e.getId());
        } catch (Exception e) {
            throw new UnhandledServerException(
                    String.format("Failed to fetch schema versions for topic : '%s'", topicName), e);
        }
    }

    @POST
    @Path("/{topicName}/schema/mapping")
    @ApiOperation(value = "Register a schema name for a kafka topic name",
            response = Long.class,
            tags = DESCRIPTION)
    @Timed
    @UnitOfWork
    public Long registerTopicSchemaMeta(@PathParam("topicName") String topicName,
                                        TopicSchemaMapping schemaMetaForTopic) {
        return schemaRegistryService.registerTopicSchemaMapping(topicName, schemaMetaForTopic);
    }

    @GET
    @Path("/{topicName}/schema/mapping")
    @ApiOperation(value = "Schema mapping for a given kafka topic name",
            response = TopicSchemaMapping.class,
            tags = DESCRIPTION)
    @Timed
    @UnitOfWork
    public TopicSchemaMapping getSchemaMetaForTopic(@PathParam("topicName") String topicName) {
        return schemaRegistryService.getTopicSchemaMeta(topicName)
                                    // EntityNotFoundException should take better convention to build error message
                                    .orElseThrow(() -> EntityNotFoundException.byName(topicName));
    }

}
