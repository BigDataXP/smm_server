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
package com.hortonworks.smm.kafka.webservice.resources.management;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.hortonworks.smm.kafka.services.clients.ClientState;
import com.hortonworks.smm.kafka.services.clients.ConsumerGroupManagementService;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerGroupInfo;
import com.hortonworks.smm.kafka.services.clients.dtos.ConsumerInfo;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.util.Collection;
import java.util.Objects;

@Path("/api/v1/admin/consumers")
@Api(description = "End point for getting consumer groups details.")
@Produces(MediaType.APPLICATION_JSON)
public class ConsumerManagementResource {

    private static final String DESCRIPTION = "Consumer group related details.";

    private final ConsumerGroupManagementService consumerGroupManagementService;
    private final SMMAuthorizer authorizer;

    @Inject
    public ConsumerManagementResource(ConsumerGroupManagementService consumerGroupManagementService,
                                      SMMAuthorizer authorizer) {
        Objects.requireNonNull(consumerGroupManagementService, "consumerGroupManagementService must not be null");
        Objects.requireNonNull(authorizer, "authorizer must not be null");

        this.consumerGroupManagementService = consumerGroupManagementService;
        this.authorizer = authorizer;
    }

    @GET
    @Path("/groupNames")
    @ApiOperation(value = "Get all consumer group names.",
            response = String.class,
            responseContainer = "List",
            tags = DESCRIPTION)
    @Timed
    public Collection<String> getConsumerGroupNames(@Context SecurityContext securityContext) {
        return SecurityUtil.filterGroups(authorizer,
                                         securityContext,
                                         consumerGroupManagementService.consumerGroupNames(),
                                         group -> group);
    }

    @GET
    @Path("/groups")
    @ApiOperation(value = "Get all consumer group details.",
            response = ConsumerGroupInfo.class,
            responseContainer = "List",
            tags = DESCRIPTION)
    @Timed
    public Collection<ConsumerGroupInfo> getConsumerGroups(@ApiParam(value = "State of the Consumer Group",
            allowableValues = "active, inactive, all") @QueryParam("state") String stateStr,
                                                           @Context SecurityContext securityContext) {
        return SecurityUtil.filterGroups(authorizer,
                                         securityContext,
                                         consumerGroupManagementService.consumerGroups(ClientState.from(stateStr)),
                                         ConsumerGroupInfo::id);
    }

    @GET
    @Path("/groups/{groupName}")
    @ApiOperation(value = "Get consumer group details for the given groupName `groupName`.",
            response = ConsumerGroupInfo.class,
            tags = DESCRIPTION)
    @Timed
    public ConsumerGroupInfo getConsumerGroupInfo(@ApiParam("Name of the consumer group")
                                                  @PathParam("groupName") String groupName,
                                                  @Context SecurityContext securityContext) {
        ConsumerGroupInfo consumerGroupInfo = null;
        if (SecurityUtil.authorizeGroupDescribe(authorizer, securityContext, groupName)) {
            consumerGroupInfo = consumerGroupManagementService.consumerGroup(groupName);
        }

        if (consumerGroupInfo == null) {
            throw new javax.ws.rs.NotFoundException(groupName + " group not found");
        }
        return consumerGroupInfo;
    }

    @GET
    @Path("/clients")
    @ApiOperation(value = "Get consumer group details for all clients.",
            response = ConsumerInfo.class,
            responseContainer = "List",
            tags = DESCRIPTION)
    @Timed
    public Collection<ConsumerInfo> getAllConsumerInfo() {
        return consumerGroupManagementService.allConsumerInfo();
    }

    @GET
    @Path("/clients/{clientId}")
    @ApiOperation(value = "Get consumer group details for a given clientId.",
            response = ConsumerInfo.class,
            tags = DESCRIPTION)
    @Timed
    public ConsumerInfo getConsumerInfo(@ApiParam("Consumer client identifier")
                                        @PathParam("clientId") String clientId) {
        return consumerGroupManagementService.consumerInfo(clientId);
    }
}
