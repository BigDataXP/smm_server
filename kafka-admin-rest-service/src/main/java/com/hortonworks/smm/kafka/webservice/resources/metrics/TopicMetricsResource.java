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
package com.hortonworks.smm.kafka.webservice.resources.metrics;

import com.codahale.metrics.annotation.Timed;
import com.google.inject.Inject;
import com.hortonworks.smm.kafka.services.metric.MetricsService;
import com.hortonworks.smm.kafka.services.metric.TimeSpan;
import com.hortonworks.smm.kafka.services.metric.dtos.TopicMetrics;
import com.hortonworks.smm.kafka.services.metric.dtos.TopicPartitionMetrics;
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
import java.util.Objects;

import static com.hortonworks.smm.kafka.services.metric.TimeSpan.ALLOWED_VALUES;

/**
 *
 */
@Path("/api/v1/admin/metrics/topics")
@Api(description = "End point for getting metrics about topics")
@Produces(MediaType.APPLICATION_JSON)
public class TopicMetricsResource {

    private static final String DESCRIPTION = "Topic metrics operations";
    private final MetricsService metricsService;
    private final SMMAuthorizer authorizer;

    @Inject
    public TopicMetricsResource(MetricsService metricsService, SMMAuthorizer authorizer) {
        Objects.requireNonNull(metricsService, "metricsService can not be null");
        Objects.requireNonNull(authorizer, "authorizer must not be null");

        this.metricsService = metricsService;
        this.authorizer = authorizer;
    }

    @GET
    @Path("/{topicName}")
    @ApiOperation(value = "Get topic metrics for the given topic.",
            response = TopicMetrics.class,
            tags = DESCRIPTION)
    @Timed
    public TopicMetrics getTopicMetrics(@ApiParam("Name of the topic") @PathParam("topicName") String topicName,
                                        @ApiParam(value = "Pre-defined Time duration. Supply either 'duration' [OR] 'from' & 'to' params",
                                                allowableValues = ALLOWED_VALUES)
                                                @QueryParam("duration") String duration,
                                        @ApiParam("Beginning of the time period. Provide '-1' to fetch the latest value")
                                                @QueryParam("from") Long from,
                                        @ApiParam("End of the time period. Provide '-1' to fetch the latest value")
                                                @QueryParam("to") Long to,
                                        @Context SecurityContext securityContext) {
        if (SecurityUtil.authorizeTopicDescribe(authorizer, securityContext, topicName)) {
            TimeSpan timeSpan = TimeSpan.from(duration, from, to);
            return TopicMetrics.from(topicName, metricsService, timeSpan);
        } else {
            throw new javax.ws.rs.NotFoundException(topicName + " topic not found");
        }
    }

    @GET
    @Path("/{topicName}/{partitionNum}")
    @ApiOperation(value = "Get topic-partition metrics for the given topic-partition.",
            response = TopicPartitionMetrics.class,
            tags = DESCRIPTION)
    @Timed
    public TopicPartitionMetrics getTopicPartitionMetrics(@ApiParam("Name of the topic") @PathParam("topicName") String topicName,
                                                          @ApiParam("Partition number") @PathParam("partitionNum") Integer partitionNum,
                                                          @ApiParam(value = "Pre-defined Time duration. Supply either 'duration' [OR] 'from' & 'to' params",
                                                                  allowableValues = ALLOWED_VALUES)
                                                          @QueryParam("duration") String duration,
                                                          @ApiParam("Beginning of the time period. Provide '-1' to fetch the latest value")
                                                          @QueryParam("from") Long from,
                                                          @ApiParam("End of the time period. Provide '-1' to fetch the latest value")
                                                          @QueryParam("to") Long to,
                                                          @Context SecurityContext securityContext) {
        if (SecurityUtil.authorizeTopicDescribe(authorizer, securityContext, topicName)) {
            TimeSpan timeSpan = TimeSpan.from(duration, from, to);
            return TopicPartitionMetrics.from(topicName, partitionNum, metricsService, timeSpan);
        } else {
            throw new javax.ws.rs.NotFoundException(topicName + " topic not found");
        }
    }
}
