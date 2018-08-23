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

package com.hortonworks.smm.kafka.webservice.resources.message;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.hortonworks.registries.common.transaction.UnitOfWork;
import com.hortonworks.smm.kafka.services.message.TopicMessageService;
import com.hortonworks.smm.kafka.services.message.dtos.TopicContent;
import com.hortonworks.smm.kafka.services.message.dtos.TopicOffsetInfo;
import com.hortonworks.smm.kafka.services.security.SMMAuthorizer;
import com.hortonworks.smm.kafka.services.security.SecurityUtil;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;
import java.util.Objects;

@Path("/api/v1/admin/topics")
@Api(tags = "Topic consumption operations", description = "Endpoint for getting topic messages.")
@Produces(MediaType.APPLICATION_JSON)
public class TopicMessageResource {

    private TopicMessageService topicMessageService;
    private final SMMAuthorizer authorizer;

    @Inject
    public TopicMessageResource(TopicMessageService topicMessageService, SMMAuthorizer authorizer) {
        Objects.requireNonNull(topicMessageService, "topicMessageService must not be null");
        Objects.requireNonNull(authorizer, "authorizer must not be null");

        this.topicMessageService = topicMessageService;
        this.authorizer = authorizer;
    }

    @GET
    @Path("/{topicName}/offsets")
    @ApiOperation(value = "Get offsets for all the partition for a given kafka topic name",
            response = TopicOffsetInfo.class)
    @Timed
    public TopicOffsetInfo getTopicOffsets(@PathParam("topicName") String topicName, @Context SecurityContext securityContext) {
        if (SecurityUtil.authorizeTopicDescribe(authorizer, securityContext, topicName)) {
            return topicMessageService.getOffset(topicName);
        } else {
            throw new javax.ws.rs.NotFoundException(topicName + " topic not found");
        }
    }

    @GET
    @Path("/{topicName}/partition/{partitionId}/payloads")
    @ApiOperation(value = "Get topic content for a given topic, its partition and the offset range with in the partition",
            response = TopicContent.class)
    @Timed
    @UnitOfWork
    public TopicContent getTopicContent(@PathParam("topicName") String topicName,
                                        @PathParam("partitionId") int partitionId,
                                        @QueryParam("startOffset") long startOffset,
                                        @QueryParam("endOffset") long endOffset,
                                        @QueryParam("keyDeserializer") String keyDeserializerClass,
                                        @QueryParam("valueDeserializer") String valueDeserializerClass,
                                        @QueryParam("responseWaitTimeInMs") Long responseWaitTimeInMs,
                                        @Context SecurityContext securityContext) {
        if (!SecurityUtil.authorizeTopicRead(authorizer, securityContext, topicName)) {
            throw new javax.ws.rs.NotFoundException(topicName + " topic not found");
        }

        return responseWaitTimeInMs != null
               ? topicMessageService.getTopicContent(topicName, partitionId, startOffset, endOffset,
                                                     keyDeserializerClass, valueDeserializerClass, responseWaitTimeInMs)
               : topicMessageService.getTopicContent(topicName, partitionId, startOffset, endOffset,
                                                     keyDeserializerClass, valueDeserializerClass);
    }
}
